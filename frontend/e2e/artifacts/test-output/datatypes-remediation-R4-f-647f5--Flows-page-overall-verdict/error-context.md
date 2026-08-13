# Instructions

- Following Playwright test failed.
- Explain why, be concise, respect Playwright best practices.
- Provide a snippet of code with the fix, if possible.

# Test info

- Name: datatypes-remediation.spec.ts >> R4 final — all three dt flows present (API + Flows page), overall verdict
- Location: e2e\datatypes-remediation.spec.ts:721:1

# Error details

```
Error: remediation failures:
json/dedup_suppression: Error: NiFi processor stats showed no dedupe activity within 420s of restart — cannot attribute a suppressed firing
json/messages_ui: skipped — an earlier phase failed
json/metrics_ui: skipped — an earlier phase failed

expect(received).toEqual(expected) // deep equality

- Expected  - 1
+ Received  + 5

- Array []
+ Array [
+   "json/dedup_suppression: Error: NiFi processor stats showed no dedupe activity within 420s of restart — cannot attribute a suppressed firing",
+   "json/messages_ui: skipped — an earlier phase failed",
+   "json/metrics_ui: skipped — an earlier phase failed",
+ ]
```

# Page snapshot

```yaml
- generic [ref=e2]:
  - region "Notifications (F8)":
    - list
  - region "Notifications alt+T"
  - generic [ref=e4]:
    - generic [ref=e8]:
      - generic [ref=e10]:
        - img [ref=e12]
        - generic [ref=e16]:
          - generic [ref=e17]: Data Mobility Platform
          - generic [ref=e18]: Adapter UI prototype
      - generic [ref=e19]:
        - generic [ref=e20]:
          - generic [ref=e21]: Workspace
          - list [ref=e23]:
            - listitem [ref=e24]:
              - link "Dashboard" [ref=e25] [cursor=pointer]:
                - /url: /
                - img [ref=e26]
                - generic [ref=e31]: Dashboard
            - listitem [ref=e32]:
              - link "Flows" [ref=e33] [cursor=pointer]:
                - /url: /flows
                - img [ref=e34]
                - generic [ref=e37]: Flows
            - listitem [ref=e38]:
              - link "Schemas" [ref=e39] [cursor=pointer]:
                - /url: /schemas
                - img [ref=e40]
                - generic [ref=e45]: Schemas
            - listitem [ref=e46]:
              - link "Application Services" [ref=e47] [cursor=pointer]:
                - /url: /application-services
                - img [ref=e48]
                - generic [ref=e51]: Application Services
            - listitem [ref=e52]:
              - link "Audit Log" [ref=e53] [cursor=pointer]:
                - /url: /audit
                - img [ref=e54]
                - generic [ref=e57]: Audit Log
        - generic [ref=e58]:
          - generic [ref=e59]: System
          - list [ref=e61]:
            - listitem [ref=e62]:
              - link "Platform Connections" [ref=e63] [cursor=pointer]:
                - /url: /connections
                - img [ref=e64]
                - generic [ref=e70]: Platform Connections
            - listitem [ref=e71]:
              - link "Proxies" [ref=e72] [cursor=pointer]:
                - /url: /apisix
                - img [ref=e73]
                - generic [ref=e76]: Proxies
      - generic [ref=e77]:
        - paragraph [ref=e80]: Connected to the live backend.
        - generic [ref=e81]:
          - generic [ref=e82]: A
          - generic [ref=e83]:
            - generic [ref=e84]: admin
            - generic [ref=e85]: Platform Admin
    - main [ref=e87]:
      - generic [ref=e88]:
        - generic [ref=e89]:
          - heading "Flows" [level=1] [ref=e90]
          - paragraph [ref=e91]: The operational console — deploy, run, and inspect every adapter flow.
        - generic [ref=e92]:
          - button "Import Connector" [ref=e93] [cursor=pointer]:
            - img
            - text: Import Connector
          - button "New Flow" [ref=e94] [cursor=pointer]:
            - img
            - text: New Flow
      - generic [ref=e96]:
        - generic [ref=e97]:
          - generic [ref=e98]:
            - img [ref=e99]
            - textbox "Search flows, entities, topics…" [active] [ref=e102]: dt
          - generic [ref=e103]: 3 of 6 flows
        - table [ref=e106]:
          - rowgroup [ref=e107]:
            - row "Select all visible flows State Flow Name Entities Topics Schema Actions" [ref=e108]:
              - columnheader "Select all visible flows" [ref=e109]:
                - checkbox "Select all visible flows" [ref=e110] [cursor=pointer]
              - columnheader "State" [ref=e111]
              - columnheader "Flow Name" [ref=e112]
              - columnheader "Entities" [ref=e113]
              - columnheader "Topics" [ref=e114]
              - columnheader "Schema" [ref=e115]
              - columnheader "Actions" [ref=e116]
          - rowgroup [ref=e117]:
            - row "Select dt json products Stopped dt json products dt_product raw.dt_json_products.dt_product — Overview Start Stop Redeploy Edit flow More actions" [ref=e118]:
              - cell "Select dt json products" [ref=e119]:
                - checkbox "Select dt json products" [ref=e120] [cursor=pointer]
              - cell "Stopped" [ref=e121]:
                - generic "Stopped" [ref=e123]:
                  - img [ref=e124]
                  - generic [ref=e127]: Stopped
              - cell "dt json products" [ref=e128]:
                - generic "dt json products" [ref=e130]
              - cell "dt_product" [ref=e131]
              - cell "raw.dt_json_products.dt_product" [ref=e132]:
                - generic "raw.dt_json_products.dt_product" [ref=e133]:
                  - code [ref=e134]: raw.dt_json_products.dt_product
              - cell "—" [ref=e135]
              - cell "Overview Start Stop Redeploy Edit flow More actions" [ref=e136]:
                - generic [ref=e137]:
                  - button "Overview" [ref=e139] [cursor=pointer]:
                    - img
                  - button "Start" [ref=e141] [cursor=pointer]:
                    - img
                  - generic [ref=e142]:
                    - button "Stop" [disabled]:
                      - img
                  - button "Redeploy" [ref=e144] [cursor=pointer]:
                    - img
                  - button "Edit flow" [ref=e146] [cursor=pointer]:
                    - img
                  - button "More actions" [ref=e147] [cursor=pointer]:
                    - img
            - row "Select dt xml feed Stopped dt xml feed dt_item raw.dt_xml_feed.dt_item — Overview Start Stop Redeploy Edit flow More actions" [ref=e148]:
              - cell "Select dt xml feed" [ref=e149]:
                - checkbox "Select dt xml feed" [ref=e150] [cursor=pointer]
              - cell "Stopped" [ref=e151]:
                - generic "Stopped" [ref=e153]:
                  - img [ref=e154]
                  - generic [ref=e157]: Stopped
              - cell "dt xml feed" [ref=e158]:
                - generic "dt xml feed" [ref=e160]
              - cell "dt_item" [ref=e161]
              - cell "raw.dt_xml_feed.dt_item" [ref=e162]:
                - generic "raw.dt_xml_feed.dt_item" [ref=e163]:
                  - code [ref=e164]: raw.dt_xml_feed.dt_item
              - cell "—" [ref=e165]
              - cell "Overview Start Stop Redeploy Edit flow More actions" [ref=e166]:
                - generic [ref=e167]:
                  - button "Overview" [ref=e169] [cursor=pointer]:
                    - img
                  - button "Start" [ref=e171] [cursor=pointer]:
                    - img
                  - generic [ref=e172]:
                    - button "Stop" [disabled]:
                      - img
                  - button "Redeploy" [ref=e174] [cursor=pointer]:
                    - img
                  - button "Edit flow" [ref=e176] [cursor=pointer]:
                    - img
                  - button "More actions" [ref=e177] [cursor=pointer]:
                    - img
            - row "Select dt csv addresses Stopped dt csv addresses dt_row raw.dt_csv_addresses.dt_row — Overview Start Stop Redeploy Edit flow More actions" [ref=e178]:
              - cell "Select dt csv addresses" [ref=e179]:
                - checkbox "Select dt csv addresses" [ref=e180] [cursor=pointer]
              - cell "Stopped" [ref=e181]:
                - generic "Stopped" [ref=e183]:
                  - img [ref=e184]
                  - generic [ref=e187]: Stopped
              - cell "dt csv addresses" [ref=e188]:
                - generic "dt csv addresses" [ref=e190]
              - cell "dt_row" [ref=e191]
              - cell "raw.dt_csv_addresses.dt_row" [ref=e192]:
                - generic "raw.dt_csv_addresses.dt_row" [ref=e193]:
                  - code [ref=e194]: raw.dt_csv_addresses.dt_row
              - cell "—" [ref=e195]
              - cell "Overview Start Stop Redeploy Edit flow More actions" [ref=e196]:
                - generic [ref=e197]:
                  - button "Overview" [ref=e199] [cursor=pointer]:
                    - img
                  - button "Start" [ref=e201] [cursor=pointer]:
                    - img
                  - generic [ref=e202]:
                    - button "Stop" [disabled]:
                      - img
                  - button "Redeploy" [ref=e204] [cursor=pointer]:
                    - img
                  - button "Edit flow" [ref=e206] [cursor=pointer]:
                    - img
                  - button "More actions" [ref=e207] [cursor=pointer]:
                    - img
```

# Test source

```ts
  647 | 
  648 |     await runPhase(k, "enable_start", async () => {
  649 |       await enableAndStart(k, "24-csv");
  650 |     });
  651 | 
  652 |     await runPhase(k, "records", async () => {
  653 |       const msgs = await pollStableMessages(flowId, topic, 300_000);
  654 |       if (msgs.length === 0) {
  655 |         await captureDefectEvidence(k, flowId);
  656 |         throw new Error(`no messages on ${topic} within 300s of Start (cron ${CRON}) — defect-csv.json captured`);
  657 |       }
  658 |       const sample = parseNewestMessage(msgs);
  659 |       e.data!["firstBatchCount"] = msgs.length;
  660 |       e.data!["sample"] = sample;
  661 |       e.data!["sampleFields"] = Object.keys(sample);
  662 |       fs.writeFileSync(path.join(ART, "csv-sample-messages.json"), JSON.stringify(msgs.slice(0, 5), null, 2), "utf-8");
  663 |       saveLedger();
  664 |       const present = headerCells.filter((h) => h in sample);
  665 |       if (present.length < Math.min(3, headerCells.length)) {
  666 |         throw new Error(
  667 |           `message is not a JSON object keyed by the CSV header columns. header=${JSON.stringify(headerCells)} sampleKeys=${JSON.stringify(Object.keys(sample))}`,
  668 |         );
  669 |       }
  670 |     });
  671 |   }
  672 | 
  673 |   await runPhase(k, "messages_ui", async () => {
  674 |     await verifyMessagesTabUI(k, CSV_FLOW, topic, "25-csv-messages-ui");
  675 |   });
  676 | 
  677 |   if (e.started) {
  678 |     await stopFlowUI(k, flowId, "26-csv-stopped");
  679 |   } else if (alreadyVerified && flowId) {
  680 |     // Resumed path: make sure the prior run's flow is not left running.
  681 |     const f = (await apiFlows()).find((x) => x.id === flowId);
  682 |     if (f?.state === "Running" || f?.state === "Paused") {
  683 |       await stopFlowUI(k, flowId, "26-csv-stopped");
  684 |     } else {
  685 |       entry(k).phases["stop"] = { ok: true, note: `flow already ${f?.state ?? "unknown"} from the prior run`, at: new Date().toISOString() };
  686 |       saveLedger();
  687 |     }
  688 |   }
  689 | });
  690 | 
  691 | // =====================================================================
  692 | // R3 — XML Messages-tab UI evidence (flow already verified + stopped)
  693 | // =====================================================================
  694 | test("R3 xml — Messages tab UI evidence on the stopped dt xml feed", async () => {
  695 |   test.setTimeout(300_000);
  696 |   const k = "xml";
  697 |   const e = entry(k);
  698 |   const topic = `raw.${tokenize(XML_FLOW)}.dt_item`;
  699 |   e.flowName = XML_FLOW;
  700 |   e.topic = topic;
  701 |   saveLedger();
  702 | 
  703 |   await runPhase(k, "locate_flow", async () => {
  704 |     const f = await flowByName(XML_FLOW);
  705 |     e.flowId = f.id;
  706 |     if (!f.nifiProcessGroupId) throw new Error(`${XML_FLOW} is not deployed`);
  707 |     const msgs = await apiMessages(f.id, topic);
  708 |     e.data!["messagesOnTopic"] = msgs.length;
  709 |     saveLedger();
  710 |     if (msgs.length === 0) throw new Error(`expected run-1 records on ${topic}, found 0`);
  711 |   });
  712 | 
  713 |   await runPhase(k, "messages_ui", async () => {
  714 |     await verifyMessagesTabUI(k, XML_FLOW, topic, "35-xml-messages-ui");
  715 |   });
  716 | });
  717 | 
  718 | // =====================================================================
  719 | // R4 — final evidence + verdict
  720 | // =====================================================================
  721 | test("R4 final — all three dt flows present (API + Flows page), overall verdict", async () => {
  722 |   test.setTimeout(300_000);
  723 | 
  724 |   const flows = await apiFlows();
  725 |   fs.writeFileSync(path.join(ART, "flows-final.json"), JSON.stringify(flows, null, 2), "utf-8");
  726 |   for (const name of [JSON_FLOW, CSV_FLOW, XML_FLOW]) {
  727 |     const f = flows.find((x) => x.name === name);
  728 |     expect.soft(f, `${name} missing from GET /api/v2/flows/`).toBeTruthy();
  729 |     expect.soft(f?.nifiProcessGroupId, `${name} should be left DEPLOYED`).toBeTruthy();
  730 |     expect.soft(f?.state, `${name} should be left Stopped`).toBe("Stopped");
  731 |   }
  732 | 
  733 |   await page.goto("/flows");
  734 |   await page.getByPlaceholder("Search flows, entities, topics…").fill("dt");
  735 |   for (const name of [JSON_FLOW, CSV_FLOW, XML_FLOW]) {
  736 |     await expect(page.getByText(name, { exact: true }).first()).toBeVisible();
  737 |   }
  738 |   await page.screenshot({ path: path.join(ART, "40-final-flows-page.png"), fullPage: true });
  739 |   saveLedger();
  740 | 
  741 |   const failures: string[] = [];
  742 |   for (const [k, e] of Object.entries(LEDGER)) {
  743 |     for (const [phase, p] of Object.entries(e.phases)) {
  744 |       if (!p.ok) failures.push(`${k}/${phase}: ${p.error ?? "failed"}`);
  745 |     }
  746 |   }
> 747 |   expect(failures, `remediation failures:\n${failures.join("\n")}`).toEqual([]);
      |                                                                     ^ Error: remediation failures:
  748 | });
  749 | 
```