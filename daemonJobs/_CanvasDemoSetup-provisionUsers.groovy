/** Bootstrap users in the demo environment; this may not be needed in the live system.
 * Note, adjust the maxResults of the subject source to >500 so we can get all of them
 * without an exception
 */

import edu.internet2.middleware.grouper.app.externalSystem.WsBearerTokenExternalSystem
import edu.internet2.middleware.grouper.cfg.GrouperConfig
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import net.sf.json.JSONObject
import org.apache.http.util.EntityUtils

import edu.internet2.middleware.grouper.*
import edu.internet2.middleware.subject.*

class Globals {
    static String GROUPER_EXT_SYSTEM_CONFIG_ID = 'canvas_api'
    static String PERSON_SUBJECT_SOURCE = 'eduLDAP'
    static int CANVAS_ACCOUNT_ID = 1
    static WsBearerTokenExternalSystem ws = new WsBearerTokenExternalSystem()
    static {
        ws.setConfigId(GROUPER_EXT_SYSTEM_CONFIG_ID)
    }
}


int provisionUser(HttpClient client, HttpPost post, String uid, String sisId, String fullName) {
    String content = /{
  "user": {
    "name": "${fullName}"
  },
  "pseudonym": {
    "unique_id": "${uid}",
    "sis_user_id": "${sisId}"
  }
}/

    StringEntity entity = new StringEntity(content)
    post.setEntity(entity)
    HttpResponse response = client.execute(post)
    if (response.getStatusLine().getStatusCode() == 200) {
        String responseString = EntityUtils.toString(response.entity, "UTF-8")
        println "DEBUG: ${responseString}"
        return JSONObject.fromObject(responseString).id
    } else {
        throw new RuntimeException("Error adding [uid=${uid}, sisId=${sisId}, fullName=${fullName}] to Canvas (${response.statusCode}: ${response.statusLine})")
    }
}

//Globals.ws.test()

// find all subjects by source; blank query sort of works, but need to override sorting behavior that returns null on blank queries
GrouperConfig.retrieveConfig().propertiesOverrideMap().put("grouper.sort.subjectSets.exactOnTop", "false");
def subjects = SubjectFinder.findAll("", Globals.PERSON_SUBJECT_SOURCE)


Map<Subject, Tuple3<String, String, String>> allWithIds = [:]

subjects.each {
    String uid = it.getAttributeValue('uid')
    if (uid) {
        allWithIds.put(it, new Tuple3(uid, it.id, it.getAttributeValue('cn')))
    } else {
        println "Skipping user with blank uid: ${it}"
    }
}


HttpPost post = new HttpPost("${Globals.ws.retrieveAttributeValueFromConfig("endpoint", true)}/api/v1/accounts/${Globals.CANVAS_ACCOUNT_ID}/users");
post.addHeader("Authorization", "Bearer ${Globals.ws.retrieveAttributeValueFromConfig("accessTokenPassword", true)}")
post.addHeader("Content-Type", "application/json")


Map<Subject, Integer> provisionedIds = [:]

allWithIds.each { Subject subject, Tuple3<String, String, String> ids ->
    HttpClient client = HttpClients.createDefault();
    try {
        int id = provisionUser(client, post, ids.first, ids.second, ids.third)
        provisionedIds.put(subject, id)
    } catch (Exception e) {
        println "${e.message}"
    } finally {
        client.close()
    }
}

/** Optionally set secondary logins for linked accounts */
//HttpPost secondaryPost = new HttpPost("${Globals.ws.retrieveAttributeValueFromConfig("endpoint", true)}/api/v1/accounts/${Globals.CANVAS_ACCOUNT_ID}/logins");
//secondaryPost.addHeader("Authorization", "Bearer ${Globals.ws.retrieveAttributeValueFromConfig("accessTokenPassword", true)}")
//secondaryPost.addHeader("Content-Type", "application/json")
//
//secondaryIds.each {Subject subject, Tuple2<String, String> ids ->
//    HttpClient client = HttpClients.createDefault();
//
//    // if the main id failed, there won't be an existing id to attach to
//    try {
//        if (provisionedIds.containsKey(subject)) {
//            int id = provisionedIds.get(subject)
//            addSecondaryLogin(client, secondaryPost, id, ids.first, ids.second)
//        } else {
//            int id = provisionUser(client, post, ids.first, ids.second, subject.getAttributeValue('cn')))
//            provisionedIds.put(subject, id)
//        }
//    } catch (Exception e) {
//        println "${e.message}"
//    } finally {
//        client.close()
//    }
//}


/* Alternative; GrouperHttpClient doesn't set mime type for multipart files
HttpPost post = new HttpPost("${ws.retrieveAttributeValueFromConfig("endpoint", true)}/api/v1/accounts/${Globals.CANVAS_ACCOUNT_ID}/sis_imports");
post.addHeader("Authorization", "Bearer ${ws.retrieveAttributeValueFromConfig("accessTokenPassword", true)}")

FileBody fileBody = new FileBody(zipFile, (ContentType)ContentType.create("application/zip"), attachmentName);
MultipartEntityBuilder builder = MultipartEntityBuilder.create();
builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
builder.addPart("attachmentName", fileBody);
HttpEntity entity = builder.build();
post.setEntity(entity);

HttpClient client = HttpClients.createDefault();
HttpResponse response = client.execute(post);
*/



// Get an existing user, to see the data format
//GET /api/v1/users/:id

//GrouperHttpClient grouperHttpClient = new GrouperHttpClient()
//grouperHttpClient.assignUrl("${ws.retrieveAttributeValueFromConfig("endpoint", true)}/api/v1/users/2")
//grouperHttpClient.assignGrouperHttpMethod(GrouperHttpMethod.get)
//grouperHttpClient.addHeader("Authorization", "Bearer ${ws.retrieveAttributeValueFromConfig("accessTokenPassword", true)}")
//
//grouperHttpClient.executeRequest()
//int code = grouperHttpClient.getResponseCode()
//String json = grouperHttpClient.getResponseBody()
