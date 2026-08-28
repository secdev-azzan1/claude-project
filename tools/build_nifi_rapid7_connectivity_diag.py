import json
import os

import build_rapid7_nifi as nifi


ROOT_INGEST_GROUP = os.environ.get("NIFI_INGEST_GROUP_ID", "0a00e822-01a0-1000-68b7-f28e69779c95")
RAPID7_USER = os.environ.get("RAPID7_USER", "apiuser")
RAPID7_PASSWORD = os.environ.get("RAPID7_PASSWORD")


if not RAPID7_PASSWORD:
    raise SystemExit("RAPID7_PASSWORD env var is required")


GROOVY = r'''
import groovy.json.JsonOutput
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.HostnameVerifier
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import org.apache.nifi.processor.io.OutputStreamCallback

def input = session.get()
if (!input) return

def prop = { name -> context.getProperty(name).evaluateAttributeExpressions(input).getValue() }

def trustAll = [
    getAcceptedIssuers: { null },
    checkClientTrusted: { X509Certificate[] certs, String authType -> },
    checkServerTrusted: { X509Certificate[] certs, String authType -> }
] as X509TrustManager
def sc = SSLContext.getInstance("TLS")
sc.init(null, [trustAll] as TrustManager[], new SecureRandom())
HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory())
HttpsURLConnection.setDefaultHostnameVerifier({ hostname, session -> true } as HostnameVerifier)

def user = prop('HTTP_USERNAME')
def pass = prop('HTTP_PASSWORD')
def auth = "Basic " + "${user}:${pass}".bytes.encodeBase64().toString()
def targets = [
    [name:'nifi_direct_securado', url:'https://172.16.20.55:3780/api/3/'],
    [name:'nifi_apisix_securado', url:'https://apisix.datapasc.com/rapid7_securado/api/3/'],
    [name:'nifi_direct_asyad', url:'https://10.100.165.14:3780/api/3/'],
    [name:'nifi_apisix_asyad', url:'https://apisix.datapasc.com/rapid7_asyad/api/3/']
]

def results = []
targets.each { t ->
    def started = System.currentTimeMillis()
    try {
        def conn = new URL(t.url).openConnection()
        if (conn instanceof HttpURLConnection) {
            conn.setInstanceFollowRedirects(false)
        }
        conn.setConnectTimeout(10000)
        conn.setReadTimeout(30000)
        conn.setRequestProperty('Authorization', auth)
        conn.setRequestProperty('Accept', 'application/json')
        def code = conn.responseCode
        def body = ''
        try {
            body = (code >= 400 ? conn.errorStream : conn.inputStream)?.getText('UTF-8') ?: ''
        } catch (Exception ignored) {}
        results << [
            name: t.name,
            url: t.url,
            status: code,
            elapsed_ms: System.currentTimeMillis() - started,
            message_prefix: body.take(180)
        ]
    } catch (Exception e) {
        results << [
            name: t.name,
            url: t.url,
            status: null,
            elapsed_ms: System.currentTimeMillis() - started,
            error: e.class.name + ': ' + e.message
        ]
    }
}

def rec = [source:'nifi', checked_at_ms:System.currentTimeMillis(), results:results]
input = session.write(input, { os -> os.write(JsonOutput.prettyPrint(JsonOutput.toJson(rec)).getBytes('UTF-8')) } as OutputStreamCallback)
session.transfer(input, REL_SUCCESS)
'''


def main():
    token = nifi.login()
    pg_id = nifi.create_pg(
        token,
        ROOT_INGEST_GROUP,
        "rapid7.connectivity_diagnostic",
        320,
        1160,
        "One-shot Rapid7 connectivity diagnostic from NiFi. Safe to delete after use.",
    )
    trigger = nifi.create_processor(
        token,
        pg_id,
        "rapid7.connectivity_diagnostic__trigger",
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        0,
        0,
        {"File Size": "0B", "Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "false"},
        [],
        "1 day",
    )
    check = nifi.create_processor(
        token,
        pg_id,
        "rapid7.connectivity_diagnostic__check",
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        0,
        220,
        {"Script Body": GROOVY, "HTTP_USERNAME": RAPID7_USER, "HTTP_PASSWORD": RAPID7_PASSWORD},
        ["failure"],
        "0 sec",
        ["HTTP_PASSWORD"],
    )
    sink = nifi.create_processor(
        token,
        pg_id,
        "rapid7.connectivity_diagnostic__hold_result",
        "org.apache.nifi.processors.standard.LogAttribute",
        0,
        440,
        {},
        ["success"],
        "0 sec",
    )
    c1 = nifi.create_connection(token, pg_id, trigger, "rapid7.connectivity_diagnostic__trigger", check, "rapid7.connectivity_diagnostic__check", ["success"])
    c2 = nifi.create_connection(token, pg_id, check, "rapid7.connectivity_diagnostic__check", sink, "rapid7.connectivity_diagnostic__hold_result", ["success"])
    print(json.dumps({"process_group_id": pg_id, "trigger_id": trigger, "check_id": check, "sink_id": sink, "result_connection_id": c2}, indent=2))


if __name__ == "__main__":
    main()
