import edu.internet2.middleware.grouper.Group
import edu.internet2.middleware.grouper.GroupSave
import edu.internet2.middleware.grouper.StemSave
import edu.internet2.middleware.grouper.SubjectFinder
import edu.internet2.middleware.grouper.app.grouperTypes.GdgTypeGroupSave
import edu.internet2.middleware.grouper.misc.SaveMode
import edu.internet2.middleware.subject.Subject

Tuple2<String, String> CANVAS_APP_FOLDER = new Tuple2("app:canvas", "Canvas")
Tuple2<String, String> CANVAS_POLICY_FOLDER = new Tuple2("${CANVAS_APP_FOLDER.first}:service:policy", null)
Tuple2<String, String> MAINACCOUNT_FOLDER = new Tuple2("${CANVAS_POLICY_FOLDER.first}:1", "ExampleUniversity")
Tuple2<String, String> SUBACCOUNT_FOLDER = new Tuple2("${MAINACCOUNT_FOLDER.first}:DynamicGroups", "Dynamic Groups (subaccount)")
Tuple2<String, String> TERM_FOLDER = new Tuple2("${SUBACCOUNT_FOLDER.first}:2024FA", "2024 Fall")

static void assignObjectTypeForGroup(Group g, String type, String owner=null, String description=null) {
    new GdgTypeGroupSave().
            assignGroup(g).
            assignType(type).
            assignDataOwner(owner).
            assignMemberDescription(description).
            assignSaveMode(SaveMode.INSERT_OR_UPDATE).
            assignReplaceAllSettings(true).
            save()
}


new StemSave().
        assignName(CANVAS_APP_FOLDER.first).
        assignDisplayExtension(CANVAS_APP_FOLDER.second).
        assignDescription("Manual groups imported into Canvas").
        assignCreateParentStemsIfNotExist(true).
        save()

new StemSave().
        assignName(MAINACCOUNT_FOLDER.first).
        assignDisplayExtension(MAINACCOUNT_FOLDER.second).
        assignDescription("Main account").
        assignCreateParentStemsIfNotExist(true).
        save()

new StemSave().
        assignName(SUBACCOUNT_FOLDER.first).
        assignDisplayExtension(SUBACCOUNT_FOLDER.second).
        assignDescription("Subaccount").
        assignCreateParentStemsIfNotExist(true).
        save()

new StemSave().
        assignName(TERM_FOLDER.first).
        assignDisplayExtension(TERM_FOLDER.second).
        assignCreateParentStemsIfNotExist(true).
        save()

new StemSave().
        assignName("${TERM_FOLDER.first}:GS101").
        assignDescription("GS101 Literature Of Reformist Environmentalism In Modern Society").
        assignCreateParentStemsIfNotExist(true).
        save()

Group teacherGroup = new GroupSave().
        assignName("${TERM_FOLDER.first}:GS101:GS101_SEC100:teacher").
        assignDisplayExtension("teacher role").
        assignCreateParentStemsIfNotExist(true).
        save()

assignObjectTypeForGroup(teacherGroup, "policy")

Group studentGroup = new GroupSave().
        assignName("${TERM_FOLDER.first}:GS101:GS101_SEC100:student").
        assignDisplayExtension("student role").
        assignCreateParentStemsIfNotExist(true).
        save()

assignObjectTypeForGroup(studentGroup, "policy")

[
        '800000375',
        '800000914',
        '800000968',
        '800001329',
        '800001469',
        '800001704',
        '800001791',
        '800001799',
        '800001858',
        '800002258',
].each {
    Subject s = SubjectFinder.findById(it, true)
    studentGroup.addMember(s, false)
}

[
        '800002467',
].each {
    Subject s = SubjectFinder.findById(it, true)
    teacherGroup.addMember(s, false)
}
