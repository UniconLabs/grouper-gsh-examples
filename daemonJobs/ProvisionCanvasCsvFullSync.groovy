/**
 * This script will provision specific groups in the app:canvas folder in Grouper, by creating csv files of
 * memberships, which then get uploaded to the Canvas server using web API calls. It will look at the last run time
 * for the daemon job, and only include groups that have had membership changes since the last job start.
 *
 * Folder/group structure and conversion to Canvas objects
 *
 * app:canvas:service:policy:
 *      account(1):subaccount(2):term(3):course(4):section(5):role(6)
 *
 * (1) account, e.g. "1"
 * (2) Subaccount, e.g. "7" or "DynamicGroups"
 * (3) Term, e.g. "Groups" (TODO set a special name like "no_term" to indicate term_id is blank in export)
 * (4) course_id, with displayExtension mapping to short_name, and description mapping to long_name (default to short_name if undefined)
 *        e.g. "ATS-Tech"; display name is "ATS Tech"; description is "ATS Technology Test"
 * (5) section_id, with displayExtension mapping to name; e.g. ATS-Tech-101
 * (6) role: must be one of the valid Canvas roles: teacher, ta, designer, student, observer
 *
 * It will only pick up groups marked with the "policy" object type
 *
 * Setup:
 *  1) So that incremental changes in membership works, set grouper.properties, groups.updateLastMembershipTime=true
 *  2) While testing, set grouper-loader.properties, example.canvasProvisioner.dryRun = true
 *  3) While testing, set grouper-loader.properties, example.canvasProvisioner.syncIfNoChanges = true
 *  4) Create script daemon job, from the source after the divider. This can be set up with the lightweight GSH option
 */


/** START groovysh_lightWeight.profile imports for development, so don't need manual imports **/
//<editor-fold desc="groovysh.profile">
import edu.internet2.middleware.grouper.*
import edu.internet2.middleware.grouper.cfg.GrouperConfig
import edu.internet2.middleware.grouper.util.*

//</editor-fold>
/** END groovysh_lightWeight.profile imports **/

// =========================  In the script, skip everything above here =========================

import edu.internet2.middleware.grouper.app.externalSystem.WsBearerTokenExternalSystem
import edu.internet2.middleware.grouper.app.grouperTypes.GrouperObjectTypesAttributeNames
import edu.internet2.middleware.grouper.app.grouperTypes.GrouperObjectTypesSettings
import edu.internet2.middleware.grouper.app.loader.GrouperLoaderConfig
import edu.internet2.middleware.grouper.app.loader.OtherJobScript
import edu.internet2.middleware.grouper.app.loader.db.Hib3GrouperLoaderLog
import edu.internet2.middleware.grouper.attr.AttributeDefName
import edu.internet2.middleware.grouper.attr.finder.AttributeDefNameFinder
import edu.internet2.middleware.grouper.exception.StemNotFoundException
import edu.internet2.middleware.grouper.hibernate.HibUtils
import edu.internet2.middleware.grouper.hibernate.HibernateSession
import edu.internet2.middleware.grouper.internal.dao.QueryOptions
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.hibernate.criterion.Criterion
import org.hibernate.criterion.Restrictions

import java.sql.Timestamp
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

String GROUPER_POLICY_FOLDER = 'app:canvas:service:policy'
String GROUPER_SUBJECT_SOURCE_ID = 'eduLDAP'
String GROUPER_EXT_SYSTEM_CONFIG_ID = 'canvas_api'
String GROUPER_CSV_ARCHIVE_DIR = '/tmp/grouper_canvas_import'

boolean FULL_SYNC_IF_NO_CHANGES = GrouperLoaderConfig.retrieveConfig().propertyValueBoolean("example.canvasProvisioner.syncIfNoChanges", false)
boolean DRY_RUN = GrouperLoaderConfig.retrieveConfig().propertyValueBoolean("example.canvasProvisioner.dryRun", false)

String randomFileNameTag() {
    return GrouperUtil.timestampToFileString(new Date()) + "_" + GrouperUtil.uniqueId()
}

CSVPrinter createCsv(OutputStream outStream) {
    CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n")

    //initialize CSVPrinter object
    return new CSVPrinter(new OutputStreamWriter(outStream), csvFileFormat)
}

Timestamp getJobLastRunStart(String jobName) {
    List<Criterion> criterionList = []

    criterionList.add(Restrictions.eq("jobName", jobName))
    criterionList.add(Restrictions.eq("status", "SUCCESS"));

    QueryOptions queryOptions = QueryOptions.create("lastUpdated", false, 1, 1)

    Criterion allCriteria = HibUtils.listCrit(criterionList)

    List<Hib3GrouperLoaderLog> loaderLogs = HibernateSession.byCriteriaStatic().
            options(queryOptions).
            list(Hib3GrouperLoaderLog.class, allCriteria)

    if (loaderLogs.size() > 0) {
        return loaderLogs[0].getStartedTime()
    } else {
        return new Timestamp(0L)
    }
}

void logMessage(Object message, Hib3GrouperLoaderLog hib3GrouperLoaderLog1) {
    println("[Canvas Provisioner] ${message.toString()}")
    hib3GrouperLoaderLog1.appendJobMessage(message.toString() + "\n")
}

/***** MAIN SCRIPT *****/

Hib3GrouperLoaderLog hib3GrouperLoaderLog = OtherJobScript.retrieveHib3GrouperLoaderLogNotNull()

/* Used for debugging directly in GSH */
//GrouperLoaderConfig.retrieveConfig().propertiesOverrideMap().put("example.canvasProvisioner.syncIfNoChanges", "true")
//GrouperLoaderConfig.retrieveConfig().propertiesOverrideMap().put("example.canvasProvisioner.dryRun", "true")
//hib3GrouperLoaderLog.jobName = "OTHER_JOB_ProvisionCanvasFullSync"


hib3GrouperLoaderLog.insertJobMessage("Starting script\n")
logMessage("GROUPER_POLICY_FOLDER = ${GROUPER_POLICY_FOLDER}", hib3GrouperLoaderLog)
logMessage("FULL_SYNC_IF_NO_CHANGES = ${FULL_SYNC_IF_NO_CHANGES}", hib3GrouperLoaderLog)
logMessage("DRY_RUN = ${DRY_RUN}", hib3GrouperLoaderLog)

Map<String, Object> outputMap = [:]

/* Get last run timestamp; note this requires property groups.updateLastMembershipTime = true */
Timestamp lastJobRunTime = getJobLastRunStart(hib3GrouperLoaderLog.jobName)

/* find all the policy groups to provision */

AttributeDefName attrDef = AttributeDefNameFinder.findByName(GrouperObjectTypesSettings.objectTypesStemName() + ":" + GrouperObjectTypesAttributeNames.GROUPER_OBJECT_TYPE_NAME, true)

Set<Group> policyGroups = new GroupFinder().
        assignScope(GROUPER_POLICY_FOLDER).
        assignIdOfAttributeDefName(attrDef.id).
        assignAttributeValuesOnAssignment(GrouperUtil.toSet('policy')).
        findGroups()


/* If aborting when no changes, check for changes since the last run. Requires grouper property groups.updateLastMembershipTime=true to work */
if (!FULL_SYNC_IF_NO_CHANGES) {
    /* there is no GroupFinder option to check membership times, so we need to loop through manually */
    Set<Group> policyGroupsModified = policyGroups.findAll {
        it.lastMembershipChange > lastJobRunTime || it.modifyTime > lastJobRunTime
    }

    /* abort if nothing has changed */
    if (policyGroupsModified.size() == 0) {
        logMessage("No membership changes were found in policy groups since last job run of ${lastJobRunTime}", hib3GrouperLoaderLog)
        hib3GrouperLoaderLog.setStatus("SUCCESS")
        return
    } else {
        logMessage("Continuing because ${policyGroupsModified.size()} group(s) modified since last job run at ${lastJobRunTime}", hib3GrouperLoaderLog)
    }
}

hib3GrouperLoaderLog.setStatus("RUNNING")

/* Collate all the policy groups per term, so batches can be isolated per term
 * Format: [account id, subaccount id, term] -> [groups...]
 */

Map<Tuple3<String, String, String>, Set<Group>> termMap = [:]
policyGroups.each {Group group ->
    String[] groupParts = group.name.replaceAll("^${GROUPER_POLICY_FOLDER}:", "").split(":")
    Tuple3 termKey = new Tuple3(groupParts[0], groupParts[1], groupParts[2])
    termMap.get(termKey, new HashSet<Group>()).add(group)
}

/* set job status to ERROR if any terms had an error */
boolean batchHasError = false

/* Loop through each account/term, create batch file and upload */
termMap.each { key, groups ->
    String accountId = key[0]
    String subaccountId = key[1]
    String termId = key[2]

    logMessage("Creating import csv for account '${accountId}', subAccount '${subaccountId}', and term '${termId}'", hib3GrouperLoaderLog)
    hib3GrouperLoaderLog.setLastUpdated(new Timestamp(System.currentTimeMillis()))

    try {
        Map<String, ByteArrayOutputStream> tempFiles = [
                'courses_groups.csv'    : new ByteArrayOutputStream(),
                'sections_groups.csv'   : new ByteArrayOutputStream(),
                'enrollments_groups.csv': new ByteArrayOutputStream(),
        ]

        CSVPrinter courseCsvPrinter = createCsv(tempFiles['courses_groups.csv'])
        CSVPrinter sectionCsvPrinter = createCsv(tempFiles['sections_groups.csv'])
        CSVPrinter enrollmentCsvPrinter = createCsv(tempFiles['enrollments_groups.csv'])

        courseCsvPrinter.printRecord(['course_id', 'short_name', 'long_name', 'account_id', 'term_id', 'status'])
        sectionCsvPrinter.printRecord(['section_id', 'course_id', 'name', 'status'])
        enrollmentCsvPrinter.printRecord(['user_id', 'role', 'section_id', 'status'])

/* Loop each policy group, parsing data based on path, and users based on membership. Multiple groups may have a common
   course and section stem, so keep track so they aren't duplicated */

        Set<Stem> courseStems = []
        Set<Stem> sectionStems = []
        int totalEnrollments = 0

        groups.each { group ->
            logMessage("Working on group: ${group.name}", hib3GrouperLoaderLog)
            outputMap.put("policyGroups", outputMap.get("policyGroups", 0) + 1)

            // trim the base folder from the beginning of the group path
            String groupPath = group.name.replaceAll("^${GROUPER_POLICY_FOLDER}:", "")

            // 0: account, 1: subaccount, 2: term, 3: course, 4: section, 5: role
            String[] groupPathParts = groupPath.split(":")

            // In case other groups incorrectly get tagged with the policy type, check for standard role type at the
            // exact folder level (types are per Canvas code, app/models/enrollment.rb)
            if (groupPathParts.size() != 6 || !groupPathParts[-1] ==~ /^(teacher|ta|designer|student|observer|)$/) {
                logMessage("WARN: Group name ${group.name} marked as a policy group in wrong location, and will be ignored", hib3GrouperLoaderLog)
                return
            }

            // retrieve the course name by reconstructing the partial path until that point
            String sectionFolderName = [GROUPER_POLICY_FOLDER, groupPathParts[0], groupPathParts[1], groupPathParts[2], groupPathParts[3], groupPathParts[4]].join(":")
            Stem sectionFolder = new StemFinder().addStemName(sectionFolderName).findStem()
            if (sectionFolder == null) {
                throw new StemNotFoundException("Unable to find section folder ${sectionFolderName}")
            }
            Stem courseFolder = sectionFolder.getParentStem()

            if (!courseStems.contains(courseFolder)) {
                // course_id, short_name, long_name, account_id, term_id, status
                courseCsvPrinter.printRecord([
                        groupPathParts[3],
                        courseFolder.displayExtension,
                        (courseFolder.description ?: courseFolder.displayExtension),
                        groupPathParts[1],
                        groupPathParts[2],
                        'active'])
                courseStems.add(courseFolder)
                outputMap.put("courses", outputMap.get("courses", 0) + 1)
            }

            if (!sectionStems.contains(sectionFolder)) {
                // section_id, course_id, name, status
                sectionCsvPrinter.printRecord([
                        groupPathParts[4],
                        groupPathParts[3],
                        sectionFolder.displayExtension,
                        'active'])
                sectionStems.add(sectionFolder)
                outputMap.put("sections", outputMap.get("sections", 0) + 1)
            }

            group.members.findAll { it.subjectSourceId == GROUPER_SUBJECT_SOURCE_ID }.collect { it.subject }.each { subject ->
                outputMap.put("members", outputMap.get("members", 0) + 1)
                String user_id = subject.id

                //println "    working on member: ${subject.id}, ${subject.name} -> Canvas ID ${user_id}"

                // user_id, role, section_id, status
                enrollmentCsvPrinter.printRecord([
                        user_id,
                        groupPathParts[5],
                        groupPathParts[4],
                        'active'
                ])
            }
        }

        courseCsvPrinter.close()
        sectionCsvPrinter.close()
        enrollmentCsvPrinter.close()

        /* Create the Zip file in memory */

        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        ZipOutputStream zos = null

        try {
            zos = new ZipOutputStream(baos)

            /* Note, fileName doesn't actually exist on disk */
            tempFiles.each { fileName, ostream ->
                ZipEntry entry = new ZipEntry(fileName);
                zos.putNextEntry(entry);
                zos.write(ostream.toByteArray())
                zos.closeEntry();
            }
        } finally {
            zos.finish()
            zos.close()
        }

/* save to archive file "instructure-xxxxx_xxxxx_xxxx.zip" */
        GrouperUtil.mkdirs(new File(GROUPER_CSV_ARCHIVE_DIR))
        String fileName = "instructure-${randomFileNameTag()}.zip"
        String filePath = "${GROUPER_CSV_ARCHIVE_DIR}/${fileName}"
        File zipFile = new File(filePath)
        zipFile.withOutputStream { stream ->
            baos.writeTo(stream)
        }

/** Upload the CSVs to Canvas
 * Requires a WSBearerToken external system to be set up via the UI or grouper-loader.properties
 *
 * grouper.wsBearerToken.canvas_api.endpoint = http://canvas:3000
 * grouper.wsBearerToken.canvas_api.accessTokenPassword = {to get a developer token, click Account -> Profile, Approved Integrations -> New access token}
 * grouper.wsBearerToken.canvas_api.testUrlSuffix = /api/v1/courses
 * grouper.wsBearerToken.canvas_api.testHttpMethod = GET
 * grouper.wsBearerToken.canvas_api.testHttpResponseCode = 200
 * grouper.wsBearerToken.canvas_api.testUrlResponseBodyRegex = \[\]
 * */

        WsBearerTokenExternalSystem ws = new WsBearerTokenExternalSystem()
        ws.setConfigId(GROUPER_EXT_SYSTEM_CONFIG_ID)

//ws.test()

        GrouperHttpClient grouperHttpClient = new GrouperHttpClient()
        //grouperHttpClient.assignUrl("${ws.retrieveAttributeValueFromConfig("endpoint", true)}/api/v1/accounts/${accountId}/sis_imports?import_type=instructure_csv&batch_mode=true&batch_mode_term_id=sis_term_id%3A${termId}&batch_mode_enrollment_drop_status=deleted&change_threshold=50")
        grouperHttpClient.assignUrl("${ws.retrieveAttributeValueFromConfig("endpoint", true)}/api/v1/accounts/${accountId}/sis_imports")
        grouperHttpClient.assignGrouperHttpMethod(GrouperHttpMethod.post)
        grouperHttpClient.addHeader("Authorization", "Bearer ${ws.retrieveAttributeValueFromConfig("accessTokenPassword", true)}")
        //grouperHttpClient.addHeader("Content-Type", "application/zip")

        grouperHttpClient.addFileToSend("attachment", zipFile)

        // The grouper client will escape and add these to the url when sending
        grouperHttpClient.addUrlParameter("batch_mode", "true")
        grouperHttpClient.addUrlParameter("batch_mode_term_id", "sis_term_id:${termId}")
        grouperHttpClient.addUrlParameter("batch_mode_enrollment_drop_status", "deleted")
        grouperHttpClient.addUrlParameter("change_threshold", "50")

        logMessage("Uploading zip file ${fileName} to Canvas API", hib3GrouperLoaderLog)
        logMessage("URL: ${grouperHttpClient.url}", hib3GrouperLoaderLog)
        grouperHttpClient.urlParameters.each {k, v ->
            logMessage("    Parameter: ${k}: ${v}", hib3GrouperLoaderLog)
        }
        grouperHttpClient.filesToSend.each {k, v ->
            logMessage("    File: ${k}: ${v}", hib3GrouperLoaderLog)
        }

        if (DRY_RUN) {
            logMessage("Skipping upload due to dry run setting", hib3GrouperLoaderLog)
        } else {
            grouperHttpClient.executeRequest()
            logMessage("URL (final): ${grouperHttpClient.url}", hib3GrouperLoaderLog)
            int code = grouperHttpClient.getResponseCode()
            String json = grouperHttpClient.getResponseBody()

            logMessage("    Api response code: ${code}", hib3GrouperLoaderLog)
            logMessage("    Api response json: ${json}", hib3GrouperLoaderLog)

            if (code != 200) {
                batchHasError = true
            }
        }
    } catch (Exception e) {
        logMessage(e.message + "", hib3GrouperLoaderLog)
        //logMessage(e.stacktrace + "", hib3GrouperLoaderLog)
        batchHasError = true
    }
}

//hib3GrouperLoaderLog.totalCount = outputMap.get("courses", 0) + outputMap.get("sections", 0) + outputMap.get("members", 0)
hib3GrouperLoaderLog.totalCount = outputMap.get("members", 0)
logMessage(GrouperUtil.mapToString(outputMap) + "", hib3GrouperLoaderLog)

hib3GrouperLoaderLog.setStatus(batchHasError ? "ERROR" : "SUCCESS")
