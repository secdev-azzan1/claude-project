import json
import os

import build_rapid7_nifi as nifi


ROOT_INGEST_GROUP = os.environ.get("NIFI_INGEST_GROUP_ID", "0a00e822-01a0-1000-68b7-f28e69779c95")
FORTISIEM_USER = os.environ.get("FORTISIEM_USER", "CMDBAPI")
FORTISIEM_PASSWORD = os.environ.get("FORTISIEM_PASSWORD")
SENTINELONE_TOKEN = os.environ.get("SENTINELONE_TOKEN", "")


if not FORTISIEM_PASSWORD:
    raise SystemExit("FORTISIEM_PASSWORD env var is required")


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

def fortiUser = prop('FORTISIEM_USERNAME')
def fortiPass = prop('FORTISIEM_PASSWORD')
def fortiAuth = "Basic " + "${fortiUser}:${fortiPass}".bytes.encodeBase64().toString()
def s1Token = prop('SENTINELONE_TOKEN')

def targets = [
    [name:'nifi_fortisiem_root_noauth', url:'https://172.16.30.6/', auth:null],
    [name:'nifi_fortisiem_rest_noauth', url:'https://172.16.30.6/phoenix/rest/', auth:null],
    [name:'nifi_fortisiem_rest_auth', url:'https://172.16.30.6/phoenix/rest/', auth:fortiAuth],
    [name:'nifi_fortisiem_apisix_noauth', url:'https://apisix.datapasc.com/fortisiem/', auth:null],
    [name:'nifi_fortisiem_apisix_auth', url:'https://apisix.datapasc.com/fortisiem/', auth:fortiAuth],
    [name:'nifi_sentinelone_base', url:'https://euce1-120-mssp.sentinelone.net/', auth:null],
    [name:'nifi_sentinelone_api_noauth', url:'https://euce1-120-mssp.sentinelone.net/web/api/v2.1/agents?limit=1', auth:null],
]
if (s1Token) {
    targets << [name:'nifi_sentinelone_api_auth', url:'https://euce1-120-mssp.sentinelone.net/web/api/v2.1/agents?limit=1', auth:"ApiToken ${s1Token}"]
}

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
        conn.setRequestProperty('Accept', 'application/json')
        if (t.auth) conn.setRequestProperty('Authorization', t.auth)
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
        "fortisiem_sentinelone.connectivity_diagnostic",
        900,
        1160,
        "One-shot FortiSIEM/SentinelOne connectivity diagnostic from NiFi. Safe to delete after use.",
    )
    trigger = nifi.create_processor(
        token,
        pg_id,
        "fortisiem_sentinelone.connectivity_diagnostic__trigger",
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        0,
        0,
        {"File Size": "0B", "Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "false"},
        [],
        "1 day",
    )
    properties = {
        "Script Body": GROOVY,
        "FORTISIEM_USERNAME": FORTISIEM_USER,
        "FORTISIEM_PASSWORD": FORTISIEM_PASSWORD,
    }
    sensitive = ["FORTISIEM_PASSWORD"]
    if SENTINELONE_TOKEN:
        properties["SENTINELONE_TOKEN"] = SENTINELONE_TOKEN
        sensitive.append("SENTINELONE_TOKEN")
    check = nifi.create_processor(
        token,
        pg_id,
        "fortisiem_sentinelone.connectivity_diagnostic__check",
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        0,
        220,
        properties,
        ["failure"],
        "0 sec",
        sensitive,
    )
    sink = nifi.create_processor(
        token,
        pg_id,
        "fortisiem_sentinelone.connectivity_diagnostic__hold_result",
        "org.apache.nifi.processors.standard.LogAttribute",
        0,
        440,
        {},
        ["success"],
        "0 sec",
    )
    c2 = nifi.create_connection(
        token,
        pg_id,
        check,
        "fortisiem_sentinelone.connectivity_diagnostic__check",
        sink,
        "fortisiem_sentinelone.connectivity_diagnostic__hold_result",
        ["success"],
    )
    nifi.create_connection(
        token,
        pg_id,
        trigger,
        "fortisiem_sentinelone.connectivity_diagnostic__trigger",
        check,
        "fortisiem_sentinelone.connectivity_diagnostic__check",
        ["success"],
    )
    print(json.dumps({"process_group_id": pg_id, "trigger_id": trigger, "check_id": check, "result_connection_id": c2}, indent=2))


if __name__ == "__main__":
    main()
