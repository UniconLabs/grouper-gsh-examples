:set verbosity QUIET

import edu.internet2.middleware.grouper.*
import edu.internet2.middleware.grouper.util.*


//------------------------- START OF DAEMON SCRIPT ----------------------------------

/**
 * Grouper group sync from Duo
 *
 * This daemon job will fetch Duo users from a Duo account, filter them based on defined criteria, and
 * keep a Grouper group in sync with the results. One use case for this is to have a basis group representing
 * users who have Duo MFA factors set up (according to specific filter criteria). Demonstrates coding for a script
 * daemon using:
 *   - logging
 *   - using external systems, to avoid credentials in the script
 *   - Duo API methods
 *   - closures as filter methods
 *   - failsafe triggers and overrides
 *   - setting log counts for total/add/update/delete/unresolvable
 *   - subject resolution
 *   - membership sync
 */

import edu.internet2.middleware.grouper.app.loader.OtherJobScript
import edu.internet2.middleware.grouper.app.loader.db.Hib3GrouperLoaderLog
import edu.internet2.middleware.grouper.internal.dao.QueryOptions
import edu.internet2.middleware.grouperDuo.DuoGrouperExternalSystem
import edu.internet2.middleware.subject.Subject
import org.apache.commons.lang.NotImplementedException
import edu.internet2.middleware.grouper.app.duo.GrouperDuoApiCommands
import edu.internet2.middleware.grouper.app.duo.GrouperDuoUser
import edu.internet2.middleware.grouper.app.loader.GrouperLoaderStatus
import edu.internet2.middleware.grouper.misc.GrouperFailsafe


class Config {

    // The Grouper group name; it must already exist, and have the right update privilege for the SERVICE_ACCOUNT_ID
    // service account defined below
    static String GROUPER_TARGET_GROUP = "basis:duo:duo_users"

    // The Grouper external system ID containing the Duo credentials
    static String DUO_EXTERNAL_SYSTEM_ID = "duo1"

    // Who should the membership changes be performed as (for locking down privileges), and audited as
    static String SERVICE_ACCOUNT_ID = "GrouperSystem"
    static String SERVICE_ACCOUNT_SOURCE_ID = "g:isa"

    // The subject source ID, used to resolve subjects by matching Duo username to subject identifier
    static String USER_SOURCE_ID = "eduLDAP" // was used for local testing

    // If it tries to delete more than this fraction of users (0.3 = 30%), abort the script instead of deleting them
    static BigDecimal SKIP_LOST_FRACTION = 0.3

    // If the count of users with bad IDs is lower than this number, the job is considered a success
    static BigDecimal NOERROR_UNRESOLVABLE_THRESHOLD = 100

    // the filter for which Duo users should be in the group. This is a closure that evaluates to true
    static Closure<Boolean> DUO_USER_FILTER = {GrouperDuoUser it ->
        /* not disabled and a member of a specific group
        //it.status != 'disabled' && it.groups.collect {it.name}.contains("main-users"))

        /* not disabled and at least one phone */
        it.status != 'disabled' && (!GrouperUtil.isBlank(it.phones))

        /* all users, no filter */
        //true
    }
}

class DuoApi {
    String externalSystemId
    String domain
    String key
    String secret
    String proxyUrl
    String proxyType

    public DuoApi(String externalSystemId) {
        this.externalSystemId = externalSystemId

        DuoGrouperExternalSystem duoSystem = new DuoGrouperExternalSystem()
        duoSystem.setConfigId(externalSystemId)

        if (duoSystem.retrieveAttributeValueFromConfig("enabled", false) == "false") {
            throw new RuntimeException("Duo external system '${externalSystemId}' is currently disabled")
        }
    }

    List<GrouperDuoUser> retrieveDuoUsers() {
        return GrouperDuoApiCommands.retrieveDuoUsers(Config.DUO_EXTERNAL_SYSTEM_ID, true)
    }
}


// a closure would be easier, but it doesn't get passed into closures inside class methods
abstract class Logger {
    abstract void log(String level, CharSequence message)
}


class PrintlnLogger extends Logger {
    void log(String level, CharSequence message) {
        if (["INFO", "WARN", "ERROR"].contains(level)) {
            println "${new Date()} - ${level} - ${message}"
        }
    }
}


class DaemonLogger extends Logger {
    Hib3GrouperLoaderLog hib3GrouperLoaderLog

    public DaemonLogger(Hib3GrouperLoaderLog hib3GrouperLoaderLog1) {
        hib3GrouperLoaderLog = hib3GrouperLoaderLog1
    }

    public DaemonLogger() {
        throw new NotImplementedException("This logger needs a Grouper loader log object")
    }

    void log(String level, CharSequence message) {
        if (["INFO", "WARN", "ERROR"].contains(level)) {
            hib3GrouperLoaderLog.appendJobMessage(message + "\n")
            hib3GrouperLoaderLog.store()
        }
    }
}


static int CountGrouperMembers(Group g) {
    QueryOptions q = new QueryOptions().retrieveResults(false).retrieveCount(true)
    Field f = FieldFinder.find("members", false)

    new MembershipFinder().addGroup(g).assignField(f).assignQueryOptionsForMember(q).findMembershipsMembers()
    // OR
    //MembershipFinder.findMembers(g, f, q)
    return q.getCount()
}

Hib3GrouperLoaderLog hib3GrouperLoaderLog = OtherJobScript.retrieveHib3GrouperLoaderLogNotNull()
def myLogger = new DaemonLogger(hib3GrouperLoaderLog)
//def myLogger = new PrintlnLogger() // for debugging
//hib3GrouperLoaderLog.jobName = "OTHER_JOB_DuoImport"

boolean hasError = false
boolean hasFailsafeError = false

myLogger.log("INFO", "START")

try {
    GrouperSession gs = GrouperSession.start(SubjectFinder.findByIdAndSource(Config.SERVICE_ACCOUNT_ID, Config.SERVICE_ACCOUNT_SOURCE_ID, true))

    DuoApi duoApi = new DuoApi(Config.DUO_EXTERNAL_SYSTEM_ID)

    List<GrouperDuoUser> duoUsers = duoApi.retrieveDuoUsers()

    List<GrouperDuoUser> filteredUsers = duoUsers.findAll(Config.DUO_USER_FILTER)

    myLogger.log("INFO", "Duo users retrieved; total count: ${duoUsers.size()}, filtered: ${filteredUsers.size()}")

    Group targetGroup = new GroupFinder().addGroupName(Config.GROUPER_TARGET_GROUP).findGroup()
    if (!targetGroup) {
        throw new RuntimeException("Could not find Grouper group: ${Config.GROUPER_TARGET_GROUP}")
    }

    List<Subject> subjects = []
    filteredUsers.each {
        Subject s = SubjectFinder.findByIdentifierAndSource(it.userName, Config.USER_SOURCE_ID, false)
        if (s) {
            subjects.add(s)
        } else {
            myLogger.log("ERROR", "Could not find subject from Duo username: ${it.userName} (Duo id: ${it.id})")
            hib3GrouperLoaderLog.incrementUnresolvableSubjectCount()
        }
    }

    int numMembersCurrent = CountGrouperMembers(targetGroup)
    int numMembersPredicted = subjects.size()
    hib3GrouperLoaderLog.totalCount = numMembersPredicted

    myLogger.log("INFO", "Start sync of ${targetGroup.name}")
    myLogger.log("INFO", "Count before: ${numMembersCurrent}")
    myLogger.log("INFO", "Count predicted: ${numMembersPredicted}")


    // Abort if losing more than 30% of the membership
    if (numMembersCurrent > 0 && (numMembersCurrent - numMembersPredicted) / numMembersCurrent > Config.SKIP_LOST_FRACTION) {
        if (GrouperFailsafe.isApproved(hib3GrouperLoaderLog.jobName)) {
            myLogger.log("INFO", "Failsafe triggered (approved for bypass) memberships to remove is more than ${100 * Config.SKIP_LOST_FRACTION}%")
            GrouperFailsafe.removeFailure(hib3GrouperLoaderLog.jobName)
        } else {
            myLogger.log("ERROR", "WARNING! Skipping membership update since memberships to remove is more than ${100 * Config.SKIP_LOST_FRACTION}%")
            GrouperFailsafe.assignFailed(hib3GrouperLoaderLog.jobName)
            hasFailsafeError = true
        }
    }

    if (!hasFailsafeError) {
        int numChanged = targetGroup.replaceMembers(subjects)
        hib3GrouperLoaderLog.updateCount = numChanged

        myLogger.log("INFO", "Count after: ${CountGrouperMembers(targetGroup)}")
    }

    myLogger.log("INFO","End sync of ${targetGroup.name}")
} catch (Exception e) {
    myLogger.log("ERROR", e.message)
    hasError = true
}

if (hib3GrouperLoaderLog.unresolvableSubjectCount >= Config.NOERROR_UNRESOLVABLE_THRESHOLD) {
    hasError = true
    myLogger.log("ERROR", "Setting job status to Error since unresolvable count of ${hib3GrouperLoaderLog.unresolvableSubjectCount} reached threshold of ${Config.NOERROR_UNRESOLVABLE_THRESHOLD}")
}

if (hasFailsafeError) {
    hib3GrouperLoaderLog.setStatus(GrouperLoaderStatus.ERROR_FAILSAFE.name())
} else if (hasError) {
    hib3GrouperLoaderLog.setStatus(GrouperLoaderStatus.ERROR.name())
} else {
    hib3GrouperLoaderLog.setStatus(GrouperLoaderStatus.SUCCESS.name())
}
