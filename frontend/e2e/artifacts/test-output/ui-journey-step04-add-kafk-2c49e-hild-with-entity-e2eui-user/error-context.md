# Instructions

- Following Playwright test failed.
- Explain why, be concise, respect Playwright best practices.
- Provide a snippet of code with the fix, if possible.

# Test info

- Name: ui-journey.spec.ts >> step04 add kafka·write child with entity e2eui_user
- Location: e2e\ui-journey.spec.ts:212:1

# Error details

```
TimeoutError: locator.click: Timeout 30000ms exceeded.
Call log:
  - waiting for getByRole('button', { name: /Entity & derived names/ })
    - locator resolved to <button disabled type="button" data-disabled="" id="radix-:r5k:" data-state="open" aria-expanded="true" aria-controls="radix-:r5l:" data-orientation="vertical" data-radix-collection-item="" class="flex flex-1 items-center justify-between font-medium transition-all [&[data-state=open]>svg]:rotate-180 py-3 hover:no-underline cursor-default [&>svg]:hidden">…</button>
  - attempting click action
    2 × waiting for element to be visible, enabled and stable
      - element is not enabled
    - retrying click action
    - waiting 20ms
    2 × waiting for element to be visible, enabled and stable
      - element is not enabled
    - retrying click action
      - waiting 100ms
    57 × waiting for element to be visible, enabled and stable
       - element is not enabled
     - retrying click action
       - waiting 500ms

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
              - link "APISIX Gateway" [ref=e72] [cursor=pointer]:
                - /url: /apisix
                - img [ref=e73]
                - generic [ref=e76]: APISIX Gateway
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
          - heading "e2eui users" [level=1] [ref=e90]
          - paragraph [ref=e91]: Adapter-based flow
        - button "Validate (1)" [ref=e93] [cursor=pointer]:
          - img
          - text: Validate (1)
      - generic [ref=e94]:
        - generic [ref=e95]:
          - generic [ref=e96]:
            - generic "Draft" [ref=e97]:
              - img [ref=e98]
              - text: Draft
            - button "Save" [ref=e101] [cursor=pointer]:
              - img
              - text: Save
            - button "Deploy" [disabled]:
              - img
              - text: Deploy
            - button "More" [ref=e102] [cursor=pointer]:
              - img
              - text: More
            - generic [ref=e103]:
              - generic [ref=e104]: Unsaved changes
              - generic [ref=e105]: Never deployed
              - generic [ref=e106]:
                - text: DLQ
                - code [ref=e107]: dlq.e2eui_users
                - 'button "About: Dead-letter queue" [ref=e108] [cursor=pointer]':
                  - img [ref=e109]
          - paragraph [ref=e112]: Deploy unavailable — Save the draft first.
        - generic [ref=e113]:
          - generic [ref=e114]:
            - generic [ref=e115]:
              - generic [ref=e116]:
                - 'heading "Flow map About: What the map can do" [level=2] [ref=e117]':
                  - text: Flow map
                  - 'button "About: What the map can do" [ref=e118] [cursor=pointer]':
                    - img [ref=e119]
                - paragraph [ref=e121]: Selecting a node opens its form. Nothing is configured on the canvas.
              - button "Hide" [ref=e122] [cursor=pointer]:
                - img
                - text: Hide
            - application [ref=e125]:
              - generic [ref=e126]:
                - generic [ref=e127]:
                  - generic:
                    - generic:
                      - img:
                        - group "Edge from b-ra094q to b-fyds74" [ref=e128] [cursor=pointer]
                      - img:
                        - group "Edge from b-fyds74 to t-uyg023" [ref=e133] [cursor=pointer]
                    - generic:
                      - group [ref=e136] [cursor=pointer]:
                        - button "raw.<entity missing>" [ref=e137]:
                          - img [ref=e140]
                          - generic [ref=e146]: raw.<entity missing>
                          - button "Attach a consumer or sink to raw.<entity missing>" [ref=e148]:
                            - img
                      - group [ref=e149] [cursor=pointer]:
                        - button "http · read New http read" [ref=e150]:
                          - generic [ref=e153]:
                            - img [ref=e154]
                            - generic [ref=e157]: http · read
                            - generic "cron */3 * * * * (UTC)" [ref=e159]:
                              - img [ref=e160]
                          - paragraph [ref=e163]: New http read
                          - button "Add a block after \"New http read\"" [active] [ref=e165]:
                            - img
                      - group [ref=e166] [cursor=pointer]:
                        - button "kafka · write 1 New kafka write" [ref=e167]:
                          - generic [ref=e170]:
                            - img [ref=e171]
                            - generic [ref=e177]: kafka · write
                            - generic "1 validation issue(s)" [ref=e179]: "1"
                          - paragraph [ref=e180]: New kafka write
                          - button "Add a block after \"New kafka write\"" [ref=e182]:
                            - img
                - button "Delete" [ref=e185] [cursor=pointer]:
                  - img
                  - text: Delete
              - img
              - generic "Control Panel" [ref=e186]:
                - button "Zoom In" [ref=e187] [cursor=pointer]:
                  - img [ref=e188]
                - button "Zoom Out" [ref=e190] [cursor=pointer]:
                  - img [ref=e191]
                - button "Fit View" [ref=e193] [cursor=pointer]:
                  - img [ref=e194]
              - generic:
                - paragraph: Drag a block's right dot onto empty space to add the next block · drag an edge end to move a branch · Delete removes one
            - generic [ref=e197]:
              - generic [ref=e198]:
                - heading "Destinations" [level=3] [ref=e199]
                - paragraph [ref=e200]: Every topic this flow touches, with its attached sink subscriptions.
              - generic [ref=e202]:
                - button "raw.<entity missing>" [ref=e203] [cursor=pointer]:
                  - img [ref=e204]
                  - generic [ref=e210]: raw.<entity missing>
                - generic [ref=e211]: written by New kafka write
                - generic [ref=e213]: no subscriptions
          - generic [ref=e214]:
            - generic [ref=e215]:
              - button "Flow settings" [ref=e216] [cursor=pointer]:
                - img
                - text: Flow settings
              - generic [ref=e217]:
                - img [ref=e218]
                - generic [ref=e220]: New kafka write
            - generic [ref=e223]:
              - generic [ref=e224]:
                - generic [ref=e225]:
                  - img [ref=e226]
                  - text: kafka
                  - generic [ref=e232]: · write
                - generic [ref=e233]: New kafka write
                - generic [ref=e234]:
                  - img [ref=e235]
                  - text: 1 issue
              - generic [ref=e237]:
                - generic [ref=e238]: Connection
                - generic [ref=e239]: what this block is, and what it talks to
              - generic [ref=e240]:
                - generic [ref=e241]:
                  - heading "Identity issues must stay visible" [level=3] [ref=e242]:
                    - button "Identity issues must stay visible" [disabled] [expanded] [ref=e243]:
                      - generic [ref=e244]:
                        - img [ref=e245]
                        - generic [ref=e249]: Identity
                        - generic [ref=e250]: issues must stay visible
                  - region "Identity issues must stay visible" [ref=e251]:
                    - generic [ref=e253]:
                      - paragraph [ref=e255]:
                        - img [ref=e256]
                        - text: No write without an entity, ever — set the entity label.
                      - generic [ref=e258]:
                        - generic [ref=e259]: Block name
                        - textbox [ref=e260]: New kafka write
                - generic [ref=e261]:
                  - heading "Adapter settings" [level=3] [ref=e262]:
                    - button "Adapter settings" [expanded] [ref=e263] [cursor=pointer]:
                      - generic [ref=e264]:
                        - img [ref=e265]
                        - generic [ref=e266]: Adapter settings
                      - img [ref=e267]
                  - region "Adapter settings" [ref=e269]:
                    - paragraph [ref=e271]: Schemaless JSON bytes onto the platform cluster (R6 — write home only). The topic name is derived below.
              - generic [ref=e272]:
                - generic [ref=e273]: Records
                - generic [ref=e274]: what happens to each record on the way through
              - generic [ref=e275]:
                - heading "Generic transformations none" [level=3] [ref=e277]:
                  - button "Generic transformations none" [ref=e278] [cursor=pointer]:
                    - generic [ref=e279]:
                      - img [ref=e280]
                      - generic [ref=e289]: Generic transformations
                      - generic [ref=e290]: none
                    - img [ref=e291]
                - generic [ref=e293]:
                  - img [ref=e294]
                  - generic [ref=e297]: "Nothing to sample: this block publishes, it never returns records."
              - generic [ref=e298]:
                - generic [ref=e299]: Destination
                - generic [ref=e300]: where the records end up, and what follows
              - generic [ref=e301]:
                - generic [ref=e302]:
                  - heading "Entity & derived names name problem must stay visible" [level=3] [ref=e303]:
                    - button "Entity & derived names name problem must stay visible" [disabled] [expanded] [ref=e304]:
                      - generic [ref=e305]:
                        - img [ref=e306]
                        - generic [ref=e310]: Entity & derived names
                        - generic [ref=e311]: name problem must stay visible
                  - region "Entity & derived names name problem must stay visible" [ref=e312]:
                    - generic [ref=e314]:
                      - generic [ref=e315]:
                        - generic [ref=e316]: Entity label
                        - textbox "asset · incident · order…" [ref=e317]
                        - paragraph [ref=e318]: No write without an entity, ever. One word for what the data is.
                      - generic [ref=e319]:
                        - generic [ref=e320]: Topic name
                        - code [ref=e322]: raw.<entity missing>
                        - 'textbox "Custom topic name (optional — R7). Default: raw.<entity missing>" [ref=e323]'
                        - paragraph [ref=e324]:
                          - img [ref=e325]
                          - text: Set an entity label to derive the topic name.
                        - paragraph [ref=e327]: Names are reserved before creation and freeze at deploy.
                - heading "Routing nothing follows yet" [level=3] [ref=e329]:
                  - button "Routing nothing follows yet" [ref=e330] [cursor=pointer]:
                    - generic [ref=e331]:
                      - img [ref=e332]
                      - generic [ref=e338]: Routing
                      - generic [ref=e339]: nothing follows yet
                    - img [ref=e340]
              - generic [ref=e342]:
                - paragraph [ref=e343]: Deleting takes 1 downstream node(s) with it.
                - button "Delete block" [ref=e344] [cursor=pointer]:
                  - img
                  - text: Delete block
```

# Test source

```ts
  120 |     await page.screenshot({ path: path.join(ART, `FAIL-${safe}.png`), fullPage: true }).catch(() => undefined);
  121 |   }
  122 | });
  123 | 
  124 | // ---------------------------------------------------------------- Step 1
  125 | test("step01 create HTTP service on Application Services", async () => {
  126 |   await page.goto("/application-services");
  127 |   await page.getByRole("button", { name: "Add Service" }).first().click();
  128 | 
  129 |   const dialog = page.getByRole("dialog");
  130 |   await expect(dialog.getByText("Add Application Service")).toBeVisible();
  131 |   await dialog.getByRole("button", { name: /HTTP service/ }).click();
  132 | 
  133 |   await fieldInput(dialog, "Name").fill(SERVICE_NAME);
  134 |   await fieldInput(dialog, "Base URL").fill(BASE_URL_VALUE);
  135 |   // Auth mode defaults to "None" — leave it.
  136 |   await expect(dialog.getByRole("combobox").filter({ hasText: "None" })).toBeVisible();
  137 | 
  138 |   await dialog.getByRole("button", { name: "Create Service" }).click();
  139 |   await expect(page.getByText(`Service "${SERVICE_NAME}" created`)).toBeVisible();
  140 | 
  141 |   const activeCard = page
  142 |     .locator("div.bg-card")
  143 |     .filter({ hasText: SERVICE_NAME })
  144 |     .filter({ hasNotText: "Retired" })
  145 |     .first();
  146 |   await expect(activeCard).toBeVisible();
  147 |   await expect(activeCard.getByText(BASE_URL_VALUE)).toBeVisible();
  148 |   await shot("01-service-created");
  149 | });
  150 | 
  151 | // ---------------------------------------------------------------- Step 2
  152 | test("step02 new flow named 'e2eui users'", async () => {
  153 |   await page.goto("/flows");
  154 |   await page.getByRole("button", { name: "New Flow" }).click();
  155 |   await expect(page).toHaveURL(/\/flow-builder\/new/);
  156 | 
  157 |   await page.getByPlaceholder("e.g. CrowdStrike Detections").fill(FLOW_NAME);
  158 |   await page.getByRole("button", { name: "Create & open builder" }).click();
  159 | 
  160 |   await expect(page).toHaveURL(/\/flow-builder\/(?!new)[\w-]+/);
  161 |   flowId = new URL(page.url()).pathname.split("/").pop()!;
  162 |   expect(flowId.length).toBeGreaterThan(0);
  163 | 
  164 |   await expect(page.getByText("Never deployed")).toBeVisible();
  165 |   // Trigger note: the cron control only appears once an http/jdbc root exists
  166 |   // (FlowSettingsForm: "Add a root block first…") — cron is set in step 3.
  167 |   await shot("02-flow-created");
  168 | });
  169 | 
  170 | // ---------------------------------------------------------------- Step 3
  171 | test("step03 place http·read root, service bind, full-URL auto-strip, record path, cron", async () => {
  172 |   await page.getByRole("button", { name: "Place the root" }).click();
  173 |   await page.getByRole("menuitem", { name: /http · read/ }).click();
  174 | 
  175 |   // Identity — Existing service mode (default) + pick the service.
  176 |   await expect(page.getByText("Existing service")).toBeVisible();
  177 |   await page.getByRole("combobox").filter({ hasText: "Select a service" }).click();
  178 |   await page.getByRole("option", { name: /e2eui dummyjson/ }).click();
  179 | 
  180 |   // Adapter settings: base-URL context line appears once the service is bound.
  181 |   await expect(page.getByText(/Base URL —/)).toBeVisible();
  182 |   await expect(page.getByText(BASE_URL_VALUE).first()).toBeVisible();
  183 | 
  184 |   // Method GET (default for read mode) — assert it.
  185 |   await expect(page.getByRole("combobox").filter({ hasText: "GET" }).first()).toBeVisible();
  186 | 
  187 |   // THE FIX UNDER TEST: type the FULL URL into the Path field.
  188 |   const pathInput = page.getByPlaceholder("/users");
  189 |   await pathInput.fill(`${BASE_URL_VALUE}/users`);
  190 |   await expect(page.getByText("Base URL comes from the service — kept just the path.")).toBeVisible();
  191 |   await expect(pathInput).toHaveValue("/users");
  192 |   await expect(page.getByText(/→\s*https:\/\/dummyjson\.com\/users/)).toBeVisible();
  193 |   await shot("03a-autostrip");
  194 | 
  195 |   // Response parsing: record path + split default ON.
  196 |   const splitSwitch = page.locator('label:has-text("split into records")').getByRole("switch");
  197 |   await expect(splitSwitch).toHaveAttribute("aria-checked", "true"); // default ON
  198 |   await page.getByPlaceholder("$.resources[*] (record path)").fill("$.users[*]");
  199 |   await expect(splitSwitch).toHaveAttribute("aria-checked", "true");
  200 | 
  201 |   // Cron: Flow settings -> Custom -> */3 * * * *
  202 |   await page.getByRole("button", { name: /Flow settings/ }).click();
  203 |   await page.getByRole("combobox").first().click();
  204 |   await page.getByRole("option", { name: /Custom/ }).click();
  205 |   await page.getByPlaceholder("*/15 * * * *").fill(CRON);
  206 |   await expect(page.getByText("5 fields required")).toHaveCount(0);
  207 |   await expect(page.getByText(/^Next:/)).toBeVisible();
  208 |   await shot("03-root-configured");
  209 | });
  210 | 
  211 | // ---------------------------------------------------------------- Step 4
  212 | test("step04 add kafka·write child with entity e2eui_user", async () => {
  213 |   await page.locator(".react-flow__node", { hasText: "New http read" }).first().click();
  214 |   await page.locator('button[title^="Add a block after"]').click();
  215 |   await page.getByRole("menuitem", { name: /kafka · write/ }).click();
  216 | 
  217 |   // Adding a block does not always move selection — click the new kafka node
  218 |   // on the canvas so its form opens, then expand the collapsed Entity section.
  219 |   await page.locator(".react-flow__node").filter({ hasText: "kafka · write" }).first().click();
> 220 |   // "Entity & derived names" is FORCED OPEN for a write block with no entity
      |                                                                      ^ TimeoutError: locator.click: Timeout 30000ms exceeded.
  221 |   // (the trigger renders disabled) — the input is already visible, no click.
  222 |   await page.getByPlaceholder("asset · incident · order…").fill(ENTITY);
  223 |   await expect(page.getByText(TOPIC).first()).toBeVisible();
  224 |   await shot("04-kafka-child");
  225 | });
  226 | 
  227 | // ---------------------------------------------------------------- Step 5
  228 | test("step05 block Test on the read block works on the unsaved flow", async () => {
  229 |   test.setTimeout(300_000);
  230 |   await page.locator(".react-flow__node", { hasText: "New http read" }).first().click();
  231 | 
  232 |   // Open the Test accordion section (closed by default).
  233 |   await page.locator("#block-section-test button").first().click();
  234 |   await expect(page.getByText("Testing saves the flow first.")).toBeVisible();
  235 | 
  236 |   await page.getByRole("button", { name: "Test block" }).click();
  237 | 
  238 |   // Save-before-test fix: real results, never "Deployment engine pending".
  239 |   await expect(page.getByText(/Test succeeded — \d+ sample record\(s\), nothing committed/)).toBeVisible({
  240 |     timeout: 90_000,
  241 |   });
  242 |   await expect(page.getByText("Deployment engine pending")).toHaveCount(0);
  243 |   expect.soft(await page.getByText(/Test succeeded — 10 sample record\(s\)/).count(), "expected 10 records").toBe(1);
  244 |   await expect(page.getByText("Detected fields — click to add an extraction rule:")).toBeVisible();
  245 |   await expect(page.getByText(/Response explorer — first sample record/)).toBeVisible();
  246 |   await shot("05-test-results");
  247 | });
  248 | 
  249 | // ---------------------------------------------------------------- Step 6
  250 | test("step06 save then deploy via preflight — all rows ok", async () => {
  251 |   test.setTimeout(300_000);
  252 |   await page.getByRole("button", { name: "Save", exact: true }).click();
  253 |   await expect(page.getByText("Draft saved")).toBeVisible();
  254 |   await expect(page.getByText(/Deploy unavailable/)).toHaveCount(0);
  255 | 
  256 |   await page.getByRole("button", { name: "Deploy", exact: true }).click();
  257 |   const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  258 |   await expect(dlg).toBeVisible();
  259 |   await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  260 |   expect(await dlg.locator("li").count()).toBeGreaterThanOrEqual(4);
  261 |   await expect(dlg.locator("svg.text-destructive")).toHaveCount(0); // every row ok
  262 |   await shot("06-preflight");
  263 | 
  264 |   const deployBtn = dlg.getByRole("button", { name: "Deploy" });
  265 |   await expect(deployBtn).toBeEnabled();
  266 |   await deployBtn.click();
  267 | 
  268 |   await expect(page.getByText("Deployed — the flow is built stopped")).toBeVisible({ timeout: 240_000 });
  269 |   await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });
  270 |   await shot("06b-deployed");
  271 | });
  272 | 
  273 | // ---------------------------------------------------------------- Step 7
  274 | test("step07 enable, start, wait for topic messages (API), verify Messages tab (UI)", async () => {
  275 |   test.setTimeout(780_000);
  276 | 
  277 |   // New flows are created disabled; Start is guarded by "The flow is disabled."
  278 |   await page.getByRole("button", { name: "More" }).click();
  279 |   await page.getByRole("menuitem", { name: "Enable" }).click();
  280 |   await page.keyboard.press("Escape");
  281 | 
  282 |   const startBtn = page.getByRole("button", { name: "Start", exact: true });
  283 |   await expect(startBtn).toBeEnabled({ timeout: 20_000 });
  284 |   await startBtn.click();
  285 |   await expect(page.getByText("Flow started")).toBeVisible({ timeout: 120_000 });
  286 |   await expect(page.locator('span[aria-label="Running"]').first()).toBeVisible({ timeout: 30_000 });
  287 |   await shot("07-started");
  288 | 
  289 |   // Cron wait happens OUTSIDE the browser: poll the backend for topic messages.
  290 |   const started = Date.now();
  291 |   let count = -1;
  292 |   let windowsUsed = 1;
  293 |   const pollWindow = async (ms: number) => {
  294 |     const deadline = Date.now() + ms;
  295 |     while (Date.now() < deadline) {
  296 |       count = await messagesCount();
  297 |       if (count > 0) return;
  298 |       await new Promise((r) => setTimeout(r, 10_000));
  299 |     }
  300 |   };
  301 |   await pollWindow(240_000); // ≤4 min budget
  302 |   if (count <= 0) {
  303 |     windowsUsed = 2; // noted retry per instructions
  304 |     await pollWindow(210_000);
  305 |   }
  306 |   const waitedS = Math.round((Date.now() - started) / 1000);
  307 |   fs.writeFileSync(
  308 |     path.join(ART, "07-poll-log.txt"),
  309 |     `topic=${TOPIC}\nflowId=${flowId}\nmessages=${count}\nwaited_seconds=${waitedS}\npoll_windows=${windowsUsed}\n`,
  310 |     "utf-8",
  311 |   );
  312 |   expect(count, `messages on ${TOPIC} after ${waitedS}s (${windowsUsed} poll window(s))`).toBeGreaterThan(0);
  313 | 
  314 |   // Now prove the same through the UI: Flows -> flow -> Messages tab -> topic.
  315 |   await page.goto("/flows");
  316 |   await page.getByText(FLOW_NAME).first().click();
  317 |   await expect(page.getByRole("tab", { name: "Messages" })).toBeVisible();
  318 |   await page.getByRole("tab", { name: "Messages" }).click();
  319 |   const topicSelect = page.getByRole("combobox").filter({ hasText: /raw\.|Pick a topic/ }).first();
  320 |   await topicSelect.click();
```