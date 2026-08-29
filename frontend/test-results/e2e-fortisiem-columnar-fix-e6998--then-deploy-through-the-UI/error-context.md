# Instructions

- Following Playwright test failed.
- Explain why, be concise, respect Playwright best practices.
- Provide a snippet of code with the fix, if possible.

# Test info

- Name: e2e\fortisiem-columnar-fix.spec.ts >> cfx-step02 validate (preflight) then deploy through the UI
- Location: e2e\fortisiem-columnar-fix.spec.ts:248:1

# Error details

```
Error: expect(received).toBeTruthy()

Received: undefined
```

# Page snapshot

```yaml
- generic [ref=e2]:
  - region "Notifications (F8)":
    - list
  - region "Notifications alt+T":
    - list:
      - listitem [ref=e3]:
        - img [ref=e5]
        - generic [ref=e8]: Deployed — the flow is built stopped
  - generic [ref=e10]:
    - generic [ref=e14]:
      - generic [ref=e16]:
        - img [ref=e18]
        - generic [ref=e22]:
          - generic [ref=e23]: Data Mobility Platform
          - generic [ref=e24]: Adapter UI prototype
      - generic [ref=e25]:
        - generic [ref=e26]:
          - generic [ref=e27]: Workspace
          - list [ref=e29]:
            - listitem [ref=e30]:
              - link "Dashboard" [ref=e31] [cursor=pointer]:
                - /url: /
                - img [ref=e32]
                - generic [ref=e37]: Dashboard
            - listitem [ref=e38]:
              - link "Flows" [ref=e39] [cursor=pointer]:
                - /url: /flows
                - img [ref=e40]
                - generic [ref=e43]: Flows
            - listitem [ref=e44]:
              - link "Schemas" [ref=e45] [cursor=pointer]:
                - /url: /schemas
                - img [ref=e46]
                - generic [ref=e51]: Schemas
            - listitem [ref=e52]:
              - link "Application Services" [ref=e53] [cursor=pointer]:
                - /url: /application-services
                - img [ref=e54]
                - generic [ref=e57]: Application Services
            - listitem [ref=e58]:
              - link "Audit Log" [ref=e59] [cursor=pointer]:
                - /url: /audit
                - img [ref=e60]
                - generic [ref=e63]: Audit Log
        - generic [ref=e64]:
          - generic [ref=e65]: System
          - list [ref=e67]:
            - listitem [ref=e68]:
              - link "Platform Connections" [ref=e69] [cursor=pointer]:
                - /url: /connections
                - img [ref=e70]
                - generic [ref=e76]: Platform Connections
            - listitem [ref=e77]:
              - link "Proxies" [ref=e78] [cursor=pointer]:
                - /url: /apisix
                - img [ref=e79]
                - generic [ref=e82]: Proxies
      - generic [ref=e83]:
        - paragraph [ref=e86]: Connected to the live backend.
        - generic [ref=e87]:
          - generic [ref=e88]: A
          - generic [ref=e89]:
            - generic [ref=e90]: admin
            - generic [ref=e91]: Platform Admin
    - main [ref=e93]:
      - generic [ref=e94]:
        - generic [ref=e95]:
          - heading "FortiSIEM Pagination Test" [level=1] [ref=e96]
          - paragraph [ref=e97]: Adapter-based flow
        - button "Validate" [ref=e99] [cursor=pointer]:
          - img
          - text: Validate
      - generic [ref=e100]:
        - generic [ref=e102]:
          - generic "Stopped" [ref=e103]:
            - img [ref=e104]
            - text: Stopped
          - button "Save" [disabled]:
            - img
            - text: Save
          - button "Deploy" [ref=e108] [cursor=pointer]:
            - img
            - text: Deploy
          - button "Start" [disabled]:
            - img
            - text: Start
          - button "Pause" [disabled]:
            - img
            - text: Pause
          - button "Stop" [disabled]:
            - img
            - text: Stop
          - button "More" [ref=e109] [cursor=pointer]:
            - img
            - text: More
          - generic [ref=e110]:
            - generic [ref=e111]: Deployed once
            - generic [ref=e112]:
              - text: DLQ
              - code [ref=e113]: dlq.fortisiem_pagination_test
              - 'button "About: Dead-letter queue" [ref=e114] [cursor=pointer]':
                - img [ref=e115]
        - generic [ref=e117]:
          - generic [ref=e118]:
            - generic [ref=e119]:
              - generic [ref=e120]:
                - 'heading "Flow map About: What the map can do" [level=2] [ref=e121]':
                  - text: Flow map
                  - 'button "About: What the map can do" [ref=e122] [cursor=pointer]':
                    - img [ref=e123]
                - paragraph [ref=e125]: Selecting a node opens its form. Nothing is configured on the canvas.
              - button "Hide" [ref=e126] [cursor=pointer]:
                - img
                - text: Hide
            - application [ref=e129]:
              - generic [ref=e130]:
                - generic [ref=e131]:
                  - generic:
                    - generic:
                      - img:
                        - group "Edge from b-utrhfu to b-flz7ij" [ref=e132] [cursor=pointer]
                      - img:
                        - group "Edge from b-flz7ij to t-tw31ag" [ref=e135] [cursor=pointer]
                    - generic:
                      - group [ref=e138] [cursor=pointer]:
                        - button "raw.fortisiem_pagination_test.event_pulling" [ref=e139]:
                          - img [ref=e142]
                          - generic [ref=e148]: raw.fortisiem_pagination_test.event_pulling
                          - button "Attach a consumer or sink to raw.fortisiem_pagination_test.event_pulling" [ref=e150]:
                            - img
                      - group [ref=e151] [cursor=pointer]:
                        - 'button "http · write New http write entity: event_pulling" [ref=e152]':
                          - generic [ref=e155]:
                            - img [ref=e156]
                            - generic [ref=e159]: http · write
                            - generic "cron */2 * * * * (UTC)" [ref=e161]:
                              - img [ref=e162]
                          - paragraph [ref=e165]: New http write
                          - generic [ref=e167]: "entity: event_pulling"
                          - button "Add a block after \"New http write\"" [ref=e169]:
                            - img
                      - group [ref=e170] [cursor=pointer]:
                        - 'button "kafka · write New kafka write entity: event_pulling" [ref=e171]':
                          - generic [ref=e174]:
                            - img [ref=e175]
                            - generic [ref=e181]: kafka · write
                          - paragraph [ref=e182]: New kafka write
                          - generic [ref=e184]: "entity: event_pulling"
                          - button "Add a block after \"New kafka write\"" [ref=e186]:
                            - img
                - button "Delete" [ref=e189] [cursor=pointer]:
                  - img
                  - text: Delete
              - img
              - generic "Control Panel" [ref=e190]:
                - button "Zoom In" [ref=e191] [cursor=pointer]:
                  - img [ref=e192]
                - button "Zoom Out" [ref=e194] [cursor=pointer]:
                  - img [ref=e195]
                - button "Fit View" [ref=e197] [cursor=pointer]:
                  - img [ref=e198]
              - generic:
                - paragraph: Drag a block's right dot onto empty space to add the next block · drag an edge end to move a branch · Delete removes one
            - generic [ref=e201]:
              - generic [ref=e202]:
                - heading "Destinations" [level=3] [ref=e203]
                - paragraph [ref=e204]: Every topic this flow touches, with its attached sink subscriptions.
              - generic [ref=e206]:
                - button "raw.fortisiem_pagination_test.event_pulling" [ref=e207] [cursor=pointer]:
                  - img [ref=e208]
                  - generic [ref=e214]: raw.fortisiem_pagination_test.event_pulling
                - generic [ref=e215]: written by New kafka write
                - generic [ref=e217]: no subscriptions
          - generic [ref=e218]:
            - generic [ref=e219]:
              - button "Flow settings" [ref=e220] [cursor=pointer]:
                - img
                - text: Flow settings
              - generic [ref=e221]:
                - img [ref=e222]
                - generic [ref=e224]: New http write
            - generic [ref=e227]:
              - generic [ref=e228]:
                - generic [ref=e229]:
                  - img [ref=e230]
                  - text: http
                  - generic [ref=e233]: · write
                - generic [ref=e234]: New http write
                - button "1 branch · 1 unconditional" [ref=e235] [cursor=pointer]
              - generic [ref=e236]:
                - generic [ref=e237]: Connection
                - generic [ref=e238]: what this block is, and what it talks to
              - generic [ref=e239]:
                - generic [ref=e240]:
                  - heading "Identity" [level=3] [ref=e241]:
                    - button "Identity" [expanded] [ref=e242] [cursor=pointer]:
                      - generic [ref=e243]:
                        - img [ref=e244]
                        - generic [ref=e248]: Identity
                      - img [ref=e249]
                  - region "Identity" [ref=e251]:
                    - generic [ref=e253]:
                      - generic [ref=e254]:
                        - generic [ref=e255]: Block name
                        - textbox [ref=e256]: New http write
                      - generic [ref=e257]:
                        - generic [ref=e258]: Service
                        - group [ref=e259]:
                          - radio "Existing service" [checked] [ref=e260] [cursor=pointer]
                          - radio "Set up here" [ref=e261] [cursor=pointer]
                        - combobox [ref=e262] [cursor=pointer]:
                          - generic: FortiSIEM Test · rev 2
                          - img [ref=e263]
                        - paragraph [ref=e265]: rev 2 · Not Tested · credentials stored on the service
                        - paragraph [ref=e266]: Hosts and credentials always come from a saved service — never typed into a block.
                - generic [ref=e267]:
                  - heading "Adapter settings" [level=3] [ref=e268]:
                    - button "Adapter settings" [expanded] [ref=e269] [cursor=pointer]:
                      - generic [ref=e270]:
                        - img [ref=e271]
                        - generic [ref=e272]: Adapter settings
                      - img [ref=e273]
                  - region "Adapter settings" [ref=e275]:
                    - generic [ref=e277]:
                      - generic [ref=e279]:
                        - generic [ref=e280]:
                          - img [ref=e281]
                          - generic [ref=e284]:
                            - paragraph [ref=e285]: API documentation
                            - paragraph [ref=e286]: Upload an OpenAPI 3.0/3.1 JSON document to pick Method and Path from it below — optional.
                        - button "Upload" [ref=e287] [cursor=pointer]:
                          - img
                          - text: Upload
                      - paragraph [ref=e288]: Base URL — https://172.16.30.6:443 (from service "FortiSIEM Test")
                      - generic [ref=e289]:
                        - generic [ref=e290]:
                          - generic [ref=e291]: Method
                          - combobox [ref=e292] [cursor=pointer]:
                            - generic: POST
                            - img [ref=e293]
                          - paragraph [ref=e295]: Per-request method for this block.
                        - generic [ref=e296]:
                          - generic [ref=e297]: Path
                          - paragraph [ref=e298]: Appended to the service's base URL — e.g. /users. The full request URL is shown below.
                          - textbox "/users" [ref=e299]: /phoenix/rest/query/cmdb
                          - paragraph [ref=e300]: → https://172.16.30.6:443/phoenix/rest/query/cmdb
                      - generic [ref=e301]:
                        - generic [ref=e302]: Response parsing
                        - generic [ref=e303]:
                          - combobox [ref=e304] [cursor=pointer]:
                            - generic: JSON
                            - img [ref=e305]
                          - textbox "$.resources[*] (record path)" [ref=e307]: $.data[*]
                          - generic [ref=e308]:
                            - switch "split into records" [checked] [ref=e309] [cursor=pointer]
                            - text: split into records
                      - generic [ref=e310]:
                        - generic [ref=e311]:
                          - switch "Column-oriented response" [checked] [ref=e312] [cursor=pointer]
                          - text: Column-oriented response
                        - paragraph [ref=e313]: "For APIs that return rows as bare arrays instead of objects (e.g. {\"data\": [[v0, v1, ...], ...]}). Name each column in order and the app turns rows into records before splitting."
                        - generic [ref=e314]:
                          - textbox "data (rows field)" [ref=e315]: data
                          - textbox "name, ipAddress, status (columns, in order)" [ref=e316]: Discover_Status, Agent_Status, Customer_ID, Device_Type_Model, Event_Pulling_Reporter, Agent_Policy, Event_Pulling_Access_Protocol, Latest_Event_Pulling_Time, Event_Pulling_Status, Agent_Type, Device_IP, Device_Type_Vendor, Device_Name, Agent_Version, Customer_Name, Agent_Upgrade_Status, Event_Pulling_Status_Description
                      - generic [ref=e318]:
                        - heading "Advanced offset pagination · via gateway proxy · body template" [level=3] [ref=e319]:
                          - button "Advanced offset pagination · via gateway proxy · body template" [expanded] [ref=e320] [cursor=pointer]:
                            - generic [ref=e321]:
                              - img [ref=e322]
                              - generic [ref=e325]: Advanced
                              - generic [ref=e326]: offset pagination · via gateway proxy · body template
                            - img [ref=e327]
                        - region "Advanced offset pagination · via gateway proxy · body template" [ref=e329]:
                          - generic [ref=e330]:
                            - generic [ref=e331]:
                              - generic [ref=e332]: Headers
                              - button "Add header" [ref=e333] [cursor=pointer]:
                                - img
                                - text: Add header
                            - generic [ref=e334]:
                              - generic [ref=e335]: Query parameters
                              - button "Add query param" [ref=e336] [cursor=pointer]:
                                - img
                                - text: Add query param
                            - generic [ref=e337]:
                              - generic [ref=e338]: Body template
                              - 'textbox "{\"records\": ${records}}" [ref=e339]': "{\"target\":\"EVENT_PULLING\",\"selectFields\":[\"Discover_Status\",\"Agent_Status\",\"Customer_ID\",\"Device_Type_Model\",\"Event_Pulling_Reporter\",\"Agent_Policy\",\"Event_Pulling_Access_Protocol\",\"Latest_Event_Pulling_Time\",\"Event_Pulling_Status\",\"Agent_Type\",\"Device_IP\",\"Device_Type_Vendor\",\"Device_Name\",\"Agent_Version\",\"Customer_Name\",\"Agent_Upgrade_Status\",\"Event_Pulling_Status_Description\"]}"
                              - paragraph [ref=e340]: Pagination fields (start, size) are added to this body automatically — you don't need to include them.
                            - generic [ref=e341]:
                              - generic [ref=e342]: Chain continues with (R3)
                              - combobox [ref=e343] [cursor=pointer]:
                                - generic: Parsed response
                                - img [ref=e344]
                            - generic [ref=e346]:
                              - generic [ref=e347]: Pagination
                              - combobox [ref=e348] [cursor=pointer]:
                                - generic: Offset / limit
                                - img [ref=e349]
                              - generic [ref=e351]:
                                - generic [ref=e352]:
                                  - generic [ref=e353]:
                                    - generic [ref=e354]: Offset parameter
                                    - textbox "offset" [ref=e355]: start
                                  - generic [ref=e356]:
                                    - generic [ref=e357]: Limit parameter
                                    - textbox "limit" [ref=e358]: size
                                  - generic [ref=e359]:
                                    - generic [ref=e360]: Limit
                                    - textbox "500" [ref=e361]: "50"
                                - generic [ref=e362]:
                                  - generic [ref=e363]: Stop condition
                                  - combobox [ref=e364] [cursor=pointer]:
                                    - generic: Total count field
                                    - img [ref=e365]
                                  - paragraph [ref=e367]: Stop once the records seen reach a total the API reports.
                                - generic [ref=e368]:
                                  - generic [ref=e369]:
                                    - generic [ref=e370]: Where is the total count?
                                    - combobox [ref=e371] [cursor=pointer]:
                                      - generic: Response body
                                      - img [ref=e372]
                                  - generic [ref=e374]:
                                    - generic [ref=e375]: JSONPath
                                    - textbox "$.meta.total" [ref=e376]: $.totalCount
              - generic [ref=e377]:
                - generic [ref=e378]: Records
                - generic [ref=e379]: what happens to each record on the way through
              - generic [ref=e380]:
                - heading "Generic transformations none" [level=3] [ref=e382]:
                  - button "Generic transformations none" [ref=e383] [cursor=pointer]:
                    - generic [ref=e384]:
                      - img [ref=e385]
                      - generic [ref=e394]: Generic transformations
                      - generic [ref=e395]: none
                    - img [ref=e396]
                - generic [ref=e398]:
                  - img [ref=e399]
                  - generic [ref=e402]: "Writes are not test-run — a probe would POST real data to the destination. If this block forwards the parsed response, describe it by hand: there is no sampled response to explore, and pagination has to be set manually."
              - generic [ref=e403]:
                - generic [ref=e404]: Destination
                - generic [ref=e405]: where the records end up, and what follows
              - generic [ref=e406]:
                - generic [ref=e407]:
                  - heading "Entity & derived names" [level=3] [ref=e408]:
                    - button "Entity & derived names" [expanded] [ref=e409] [cursor=pointer]:
                      - generic [ref=e410]:
                        - img [ref=e411]
                        - generic [ref=e415]: Entity & derived names
                      - img [ref=e416]
                  - region "Entity & derived names" [ref=e418]:
                    - generic [ref=e421]:
                      - generic [ref=e422]: Entity label
                      - textbox "asset · incident · order…" [ref=e423]: event_pulling
                      - paragraph [ref=e424]: No write without an entity, ever. One word for what the data is.
                - generic [ref=e425]:
                  - heading "Routing" [level=3] [ref=e426]:
                    - button "Routing" [expanded] [ref=e427] [cursor=pointer]:
                      - generic [ref=e428]:
                        - img [ref=e429]
                        - generic [ref=e435]: Routing
                      - img [ref=e436]
                  - region "Routing" [ref=e438]:
                    - generic [ref=e440]:
                      - generic [ref=e442]:
                        - generic [ref=e443]:
                          - textbox "Branch names are free text — they pre-fill topic variant tokens in the naming walk." [ref=e444]:
                            - /placeholder: branch-1
                          - generic [ref=e445]: all records
                          - button "kafka · write New kafka write" [ref=e446] [cursor=pointer]:
                            - generic [ref=e447]:
                              - img [ref=e448]
                              - text: kafka
                              - generic [ref=e454]: · write
                            - generic [ref=e455]: New kafka write
                            - img [ref=e456]
                        - generic [ref=e458]:
                          - paragraph [ref=e459]: Every record takes this branch. Add a rule to send only some of them.
                          - button "Add rule" [ref=e461] [cursor=pointer]:
                            - img
                            - text: Add rule
                      - generic [ref=e462]:
                        - paragraph [ref=e463]:
                          - img [ref=e464]
                          - generic [ref=e469]: "Branches are independent: every record is tested against each branch on its own, so a record can travel down more than one. There is no order and no first-match-wins."
                        - paragraph [ref=e470]: "\"New kafka write\" has no rules, so it receives every record — including the ones the other branches also take."
                        - paragraph [ref=e471]: To add another branch, use the ＋ beside this block on the map.
              - generic [ref=e472]:
                - paragraph [ref=e473]: Deleting takes 2 downstream node(s) with it.
                - button "Delete block" [ref=e474] [cursor=pointer]:
                  - img
                  - text: Delete block
```

# Test source

```ts
  190 |   await expect(columnsInput).toHaveValue(COLUMNS.join(", "));
  191 |   await sleep(300);
  192 | 
  193 |   const rowsFieldInput = page.getByPlaceholder("data (rows field)");
  194 |   await expect(rowsFieldInput).toBeVisible();
  195 |   await rowsFieldInput.fill("data_sentinel_check");
  196 |   await expect(rowsFieldInput).toHaveValue("data_sentinel_check");
  197 |   await sleep(300);
  198 |   // Reselect (remount) now, mid-sentinel, to positively prove the sentinel
  199 |   // value is genuinely in draft state (no default collision possible here).
  200 |   await page.locator(".react-flow__node", { hasText: "New kafka write" }).first().click();
  201 |   await sleep(200);
  202 |   await page.locator(".react-flow__node", { hasText: "New http write" }).first().click();
  203 |   await expect(page.getByText(/→\s*https:\/\/172\.16\.30\.6:443/)).toBeVisible();
  204 |   const rowsFieldSentinelCheck = page.getByPlaceholder("data (rows field)");
  205 |   await expect(rowsFieldSentinelCheck).toHaveValue("data_sentinel_check", { timeout: 10_000 });
  206 |   const columnsInputSentinelCheck = page.getByPlaceholder("name, ipAddress, status (columns, in order)");
  207 |   await expect(columnsInputSentinelCheck).toHaveValue(COLUMNS.join(", "), { timeout: 10_000 });
  208 | 
  209 |   // Now set the real desired value, last, with nothing after it but the
  210 |   // remount re-check (read-only) and Save.
  211 |   await rowsFieldSentinelCheck.fill("data");
  212 |   await expect(rowsFieldSentinelCheck).toHaveValue("data");
  213 |   await sleep(300);
  214 |   await shot("01b-columnar-configured");
  215 | 
  216 |   await page.locator(".react-flow__node", { hasText: "New kafka write" }).first().click();
  217 |   await sleep(200);
  218 |   await page.locator(".react-flow__node", { hasText: "New http write" }).first().click();
  219 |   await expect(page.getByText(/→\s*https:\/\/172\.16\.30\.6:443/)).toBeVisible();
  220 | 
  221 |   const limitInputRemount = fieldInput(page, "Limit");
  222 |   if (!(await limitInputRemount.isVisible().catch(() => false))) {
  223 |     await page.locator('button:has-text("Advanced")').first().click();
  224 |   }
  225 |   await expect(limitInputRemount).toHaveValue(NEW_LIMIT, { timeout: 10_000 });
  226 |   const columnarSwitchRemount = page.locator('label:has-text("Column-oriented response")').getByRole("switch");
  227 |   await expect(columnarSwitchRemount).toHaveAttribute("aria-checked", "true");
  228 |   const rowsFieldRemount = page.getByPlaceholder("data (rows field)");
  229 |   await expect(rowsFieldRemount).toHaveValue("data");
  230 |   const columnsInputRemount = page.getByPlaceholder("name, ipAddress, status (columns, in order)");
  231 |   await expect(columnsInputRemount).toHaveValue(COLUMNS.join(", "), { timeout: 10_000 });
  232 |   await shot("01b2-columnar-confirmed-after-remount");
  233 | 
  234 |   await page.getByRole("button", { name: "Save", exact: true }).click();
  235 |   await expect(page.getByText("Draft saved")).toBeVisible({ timeout: 30_000 });
  236 |   await expect(page.getByText(/Deploy unavailable/)).toHaveCount(0);
  237 |   await shot("01c-saved");
  238 | 
  239 |   const after = await flowDoc();
  240 |   saveJson("01d-after-save.json", after);
  241 |   const block = (after.blocks ?? []).find((b: any) => b.id === "b-utrhfu");
  242 |   expect(block.config.pagination.fields.limitValue).toBe(NEW_LIMIT);
  243 |   expect(block.config.columnar).toMatchObject({ enabled: true, rowsField: "data" });
  244 |   expect(block.config.columnar.columns).toEqual(COLUMNS);
  245 | });
  246 | 
  247 | // ---------------------------------------------------------------- Step 2
  248 | test("cfx-step02 validate (preflight) then deploy through the UI", async () => {
  249 |   test.setTimeout(300_000);
  250 | 
  251 |   await page.getByRole("button", { name: "Deploy", exact: true }).click();
  252 |   const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  253 |   await expect(dlg).toBeVisible();
  254 |   await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  255 |   const rowCount = await dlg.locator("li").count();
  256 |   expect(rowCount).toBeGreaterThanOrEqual(1);
  257 |   const failingCount = await dlg.locator("svg.text-destructive").count();
  258 |   await shot("02a-preflight");
  259 |   if (failingCount > 0) {
  260 |     const rowTexts = await dlg.locator("li").allTextContents();
  261 |     saveJson("02b-preflight-failures.json", rowTexts);
  262 |     throw new Error(`Preflight dialog shows ${failingCount} failing row(s): ${rowTexts.join(" | ")}`);
  263 |   }
  264 | 
  265 |   const deployBtn = dlg.getByRole("button", { name: "Deploy" });
  266 |   await expect(deployBtn).toBeEnabled();
  267 |   await deployBtn.click();
  268 | 
  269 |   // The dialog's own checks are a client-side approximation; the REAL
  270 |   // compiler compile (including the new columnar_transform processor) only
  271 |   // runs server-side on this click. Race success vs. a compile-error toast
  272 |   // so a genuine failure is reported precisely rather than timing out blind.
  273 |   const deployed = page.getByText("Deployed — the flow is built stopped");
  274 |   const errorToast = page.locator('[data-sonner-toast][data-type="error"]');
  275 |   await Promise.race([
  276 |     deployed.waitFor({ state: "visible", timeout: 240_000 }),
  277 |     errorToast.waitFor({ state: "visible", timeout: 240_000 }),
  278 |   ]);
  279 |   if (await errorToast.isVisible().catch(() => false)) {
  280 |     const text = await errorToast.allTextContents();
  281 |     await shot("02c-deploy-error");
  282 |     throw new Error(`Deploy failed: ${text.join(" | ")}`);
  283 |   }
  284 |   await expect(deployed).toBeVisible();
  285 |   await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });
  286 |   await shot("02d-deployed");
  287 | 
  288 |   const after = await flowDoc();
  289 |   saveJson("02e-after-deploy.json", after);
> 290 |   expect(after.runtimeScopeMap?.["b-utrhfu"]?.components?.columnar_transform).toBeTruthy();
      |                                                                               ^ Error: expect(received).toBeTruthy()
  291 | });
  292 | 
  293 | // ---------------------------------------------------------------- Step 3
  294 | test("cfx-step03 enable, start (one light-touch live run), poll briefly, stop", async () => {
  295 |   test.setTimeout(420_000);
  296 | 
  297 |   await page.getByRole("button", { name: "More" }).click();
  298 |   await expect(page.getByRole("menuitem", { name: "Enable" })).toBeVisible();
  299 |   await page.getByRole("menuitem", { name: "Enable" }).click();
  300 |   await page.keyboard.press("Escape");
  301 | 
  302 |   const startBtn = page.getByRole("button", { name: "Start", exact: true });
  303 |   await expect(startBtn).toBeEnabled({ timeout: 20_000 });
  304 |   await startBtn.click();
  305 |   await expect(page.getByText("Flow started")).toBeVisible({ timeout: 120_000 });
  306 |   await shot("03a-started");
  307 | 
  308 |   // Cron is */2 * * * * — poll briefly (do NOT wait for the queue to fully
  309 |   // drain) until a few records land, then stop immediately. This is the
  310 |   // single live hit against FortiSIEM for this verification pass.
  311 |   const deadline = Date.now() + 330_000;
  312 |   let sample: unknown[] = [];
  313 |   while (Date.now() < deadline) {
  314 |     sample = await messages();
  315 |     if (sample.length > 0) break;
  316 |     await sleep(10_000);
  317 |   }
  318 |   saveJson("03b-messages-sample.json", { topic: TOPIC, count: sample.length, sample: sample.slice(0, 3) });
  319 | 
  320 |   const stopBtn = page.getByRole("button", { name: "Stop", exact: true });
  321 |   await expect(stopBtn).toBeEnabled({ timeout: 20_000 });
  322 |   await stopBtn.click();
  323 |   await expect(page.getByText(/^Stopped/)).toBeVisible({ timeout: 60_000 });
  324 |   await shot("03c-stopped");
  325 | 
  326 |   const final = await flowDoc();
  327 |   saveJson("03d-final-flow.json", final);
  328 |   saveJson("03e-verdict.json", {
  329 |     flowId: FLOW_ID,
  330 |     topic: TOPIC,
  331 |     lightPollMessageCount: sample.length,
  332 |     finalState: final.state,
  333 |     note: "Authoritative NiFi/Kafka verification is performed separately via direct REST calls, not via this app-side poll.",
  334 |   });
  335 | });
  336 | 
```