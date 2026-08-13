# Instructions

- Following Playwright test failed.
- Explain why, be concise, respect Playwright best practices.
- Provide a snippet of code with the fix, if possible.

# Test info

- Name: ui-journey.spec.ts >> step09 UI cleanup — delete flow, retire service
- Location: e2e\ui-journey.spec.ts:362:1

# Error details

```
Error: expect(locator).toBeVisible() failed

Locator: locator('div.bg-card').filter({ hasText: 'e2eui dummyjson' }).first().getByRole('button', { name: 'Reinstate' })
Expected: visible
Timeout: 20000ms
Error: element(s) not found

Call log:
  - Expect "toBeVisible" with timeout 20000ms
  - waiting for locator('div.bg-card').filter({ hasText: 'e2eui dummyjson' }).first().getByRole('button', { name: 'Reinstate' })

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
          - heading "Application Services" [level=1] [ref=e90]
          - paragraph [ref=e91]: Reusable endpoint + credential profiles. Every adapter that needs credentials selects a service — secrets never live on a block.
        - button "Add Service" [ref=e93] [cursor=pointer]:
          - img
          - text: Add Service
      - generic [ref=e94]:
        - generic [ref=e95]:
          - generic [ref=e96]:
            - img [ref=e98]
            - heading "HTTP service" [level=2] [ref=e103]
            - generic [ref=e104]: · 18 services — Base URL + authentication profile used by http adapter blocks.
          - generic [ref=e105]:
            - generic [ref=e106]:
              - generic [ref=e107]:
                - generic [ref=e108]:
                  - img [ref=e110]
                  - generic [ref=e115]:
                    - generic [ref=e116]:
                      - heading "T8.1 live-smoke service" [level=3] [ref=e117]
                      - generic [ref=e118]: rev 1
                      - generic "Retired" [ref=e119]:
                        - img [ref=e120]
                        - text: Retired
                    - paragraph [ref=e124]: HTTP service
                - generic "Not Tested" [ref=e125]:
                  - img [ref=e126]
                  - text: Not Tested
              - generic [ref=e128]:
                - generic [ref=e129]:
                  - generic [ref=e130]: Base URL
                  - generic [ref=e131]: https://example.invalid
                - generic [ref=e132]:
                  - generic [ref=e133]: Auth
                  - generic [ref=e134]: No auth
                - generic [ref=e135]:
                  - generic [ref=e136]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e137]: "Last tested: never"
                  - generic [ref=e138]:
                    - button "Test" [ref=e139] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e140] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e141] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e142]:
              - generic [ref=e143]:
                - generic [ref=e144]:
                  - img [ref=e146]
                  - generic [ref=e151]:
                    - generic [ref=e152]:
                      - heading "T8.1 live-smoke service" [level=3] [ref=e153]
                      - generic [ref=e154]: rev 1
                      - generic "Retired" [ref=e155]:
                        - img [ref=e156]
                        - text: Retired
                    - paragraph [ref=e160]: HTTP service
                - generic "Not Tested" [ref=e161]:
                  - img [ref=e162]
                  - text: Not Tested
              - generic [ref=e164]:
                - generic [ref=e165]:
                  - generic [ref=e166]: Base URL
                  - generic [ref=e167]: https://example.invalid
                - generic [ref=e168]:
                  - generic [ref=e169]: Auth
                  - generic [ref=e170]: No auth
                - generic [ref=e171]:
                  - generic [ref=e172]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e173]: "Last tested: never"
                  - generic [ref=e174]:
                    - button "Test" [ref=e175] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e176] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e177] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e178]:
              - generic [ref=e179]:
                - generic [ref=e180]:
                  - img [ref=e182]
                  - generic [ref=e187]:
                    - generic [ref=e188]:
                      - heading "e2ea dummyjson" [level=3] [ref=e189]
                      - generic [ref=e190]: rev 1
                      - generic "Retired" [ref=e191]:
                        - img [ref=e192]
                        - text: Retired
                    - paragraph [ref=e196]: HTTP service
                - generic "Healthy" [ref=e197]:
                  - img [ref=e198]
                  - text: Healthy
              - generic [ref=e201]:
                - generic [ref=e202]:
                  - generic [ref=e203]: Base URL
                  - generic [ref=e204]: https://dummyjson.com
                - generic [ref=e205]:
                  - generic [ref=e206]: Auth
                  - generic [ref=e207]: No auth
                - generic [ref=e208]:
                  - generic [ref=e209]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e210]: "Last tested: 8h ago"
                  - generic [ref=e211]:
                    - button "Test" [ref=e212] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e213] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e214] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e215]:
              - generic [ref=e216]:
                - generic [ref=e217]:
                  - img [ref=e219]
                  - generic [ref=e224]:
                    - generic [ref=e225]:
                      - heading "e2eb dummyjson" [level=3] [ref=e226]
                      - generic [ref=e227]: rev 1
                      - generic "Retired" [ref=e228]:
                        - img [ref=e229]
                        - text: Retired
                    - paragraph [ref=e233]: HTTP service
                - generic "Not Tested" [ref=e234]:
                  - img [ref=e235]
                  - text: Not Tested
              - generic [ref=e237]:
                - generic [ref=e238]:
                  - generic [ref=e239]: Base URL
                  - generic [ref=e240]: https://dummyjson.com
                - generic [ref=e241]:
                  - generic [ref=e242]: Auth
                  - generic [ref=e243]: No auth
                - generic [ref=e244]:
                  - generic [ref=e245]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e246]: "Last tested: never"
                  - generic [ref=e247]:
                    - button "Test" [ref=e248] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e249] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e250] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e251]:
              - generic [ref=e252]:
                - generic [ref=e253]:
                  - img [ref=e255]
                  - generic [ref=e260]:
                    - generic [ref=e261]:
                      - heading "e2ec proxied" [level=3] [ref=e262]
                      - generic [ref=e263]: rev 1
                      - generic "Retired" [ref=e264]:
                        - img [ref=e265]
                        - text: Retired
                    - paragraph [ref=e269]: HTTP service
                - generic "Not Tested" [ref=e270]:
                  - img [ref=e271]
                  - text: Not Tested
              - generic [ref=e273]:
                - generic [ref=e274]:
                  - generic [ref=e275]: Base URL
                  - generic [ref=e276]: https://dummyjson.com
                - generic [ref=e277]:
                  - generic [ref=e278]: Auth
                  - generic [ref=e279]: No auth
                - generic [ref=e280]:
                  - generic [ref=e281]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e282]: "Last tested: never"
                  - generic [ref=e283]:
                    - button "Test" [ref=e284] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e285] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e286] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e287]:
              - generic [ref=e288]:
                - generic [ref=e289]:
                  - img [ref=e291]
                  - generic [ref=e296]:
                    - generic [ref=e297]:
                      - heading "e2ec session" [level=3] [ref=e298]
                      - generic [ref=e299]: rev 1
                      - generic "Retired" [ref=e300]:
                        - img [ref=e301]
                        - text: Retired
                    - paragraph [ref=e305]: HTTP service
                - generic "Healthy" [ref=e306]:
                  - img [ref=e307]
                  - text: Healthy
              - generic [ref=e310]:
                - generic [ref=e311]:
                  - generic [ref=e312]: Base URL
                  - generic [ref=e313]: https://dummyjson.com
                - generic [ref=e314]:
                  - generic [ref=e315]: Auth
                  - generic [ref=e316]: Session token
                - generic [ref=e317]:
                  - generic [ref=e318]: Login path
                  - generic [ref=e319]: /auth/login
                - generic [ref=e320]:
                  - generic [ref=e321]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e322]: "Last tested: 8h ago"
                  - generic [ref=e323]:
                    - button "Test" [ref=e324] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e325] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e326] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e327]:
              - generic [ref=e328]:
                - generic [ref=e329]:
                  - img [ref=e331]
                  - generic [ref=e336]:
                    - generic [ref=e337]:
                      - heading "e2emain probe svc" [level=3] [ref=e338]
                      - generic [ref=e339]: rev 1
                    - paragraph [ref=e340]: HTTP service
                - generic "Not Tested" [ref=e341]:
                  - img [ref=e342]
                  - text: Not Tested
              - generic [ref=e344]:
                - generic [ref=e345]:
                  - generic [ref=e346]: Base URL
                  - generic [ref=e347]: https://dummyjson.com
                - generic [ref=e348]:
                  - generic [ref=e349]: Auth
                  - generic [ref=e350]: No auth
                - generic [ref=e351]:
                  - generic [ref=e352]:
                    - button "1 dependent flow" [ref=e353] [cursor=pointer]:
                      - img
                      - text: 1 dependent flow
                    - generic [ref=e354]: "Last tested: never"
                  - generic [ref=e355]:
                    - button "Test" [ref=e356] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e357] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Retire" [ref=e358] [cursor=pointer]:
                      - img
                      - text: Retire
            - generic [ref=e359]:
              - generic [ref=e360]:
                - generic [ref=e361]:
                  - img [ref=e363]
                  - generic [ref=e368]:
                    - generic [ref=e369]:
                      - heading "e2emain probe svc" [level=3] [ref=e370]
                      - generic [ref=e371]: rev 1
                      - generic "Retired" [ref=e372]:
                        - img [ref=e373]
                        - text: Retired
                    - paragraph [ref=e377]: HTTP service
                - generic "Not Tested" [ref=e378]:
                  - img [ref=e379]
                  - text: Not Tested
              - generic [ref=e381]:
                - generic [ref=e382]:
                  - generic [ref=e383]: Base URL
                  - generic [ref=e384]: https://dummyjson.com
                - generic [ref=e385]:
                  - generic [ref=e386]: Auth
                  - generic [ref=e387]: No auth
                - generic [ref=e388]:
                  - generic [ref=e389]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e390]: "Last tested: never"
                  - generic [ref=e391]:
                    - button "Test" [ref=e392] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e393] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e394] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e395]:
              - generic [ref=e396]:
                - generic [ref=e397]:
                  - img [ref=e399]
                  - generic [ref=e404]:
                    - generic [ref=e405]:
                      - heading "e2er dummyjson" [level=3] [ref=e406]
                      - generic [ref=e407]: rev 1
                      - generic "Retired" [ref=e408]:
                        - img [ref=e409]
                        - text: Retired
                    - paragraph [ref=e413]: HTTP service
                - generic "Not Tested" [ref=e414]:
                  - img [ref=e415]
                  - text: Not Tested
              - generic [ref=e417]:
                - generic [ref=e418]:
                  - generic [ref=e419]: Base URL
                  - generic [ref=e420]: https://dummyjson.com
                - generic [ref=e421]:
                  - generic [ref=e422]: Auth
                  - generic [ref=e423]: No auth
                - generic [ref=e424]:
                  - generic [ref=e425]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e426]: "Last tested: never"
                  - generic [ref=e427]:
                    - button "Test" [ref=e428] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e429] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e430] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e431]:
              - generic [ref=e432]:
                - generic [ref=e433]:
                  - img [ref=e435]
                  - generic [ref=e440]:
                    - generic [ref=e441]:
                      - heading "e2er session" [level=3] [ref=e442]
                      - generic [ref=e443]: rev 1
                      - generic "Retired" [ref=e444]:
                        - img [ref=e445]
                        - text: Retired
                    - paragraph [ref=e449]: HTTP service
                - generic "Healthy" [ref=e450]:
                  - img [ref=e451]
                  - text: Healthy
              - generic [ref=e454]:
                - generic [ref=e455]:
                  - generic [ref=e456]: Base URL
                  - generic [ref=e457]: https://dummyjson.com
                - generic [ref=e458]:
                  - generic [ref=e459]: Auth
                  - generic [ref=e460]: Session token
                - generic [ref=e461]:
                  - generic [ref=e462]: Login path
                  - generic [ref=e463]: /auth/login
                - generic [ref=e464]:
                  - generic [ref=e465]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e466]: "Last tested: 3h ago"
                  - generic [ref=e467]:
                    - button "Test" [ref=e468] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e469] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e470] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e471]:
              - generic [ref=e472]:
                - generic [ref=e473]:
                  - img [ref=e475]
                  - generic [ref=e480]:
                    - generic [ref=e481]:
                      - heading "e2emain session svc" [level=3] [ref=e482]
                      - generic [ref=e483]: rev 1
                      - generic "Retired" [ref=e484]:
                        - img [ref=e485]
                        - text: Retired
                    - paragraph [ref=e489]: HTTP service
                - generic "Healthy" [ref=e490]:
                  - img [ref=e491]
                  - text: Healthy
              - generic [ref=e494]:
                - generic [ref=e495]:
                  - generic [ref=e496]: Base URL
                  - generic [ref=e497]: https://dummyjson.com
                - generic [ref=e498]:
                  - generic [ref=e499]: Auth
                  - generic [ref=e500]: Session token
                - generic [ref=e501]:
                  - generic [ref=e502]: Login path
                  - generic [ref=e503]: /auth/login
                - generic [ref=e504]:
                  - generic [ref=e505]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e506]: "Last tested: 3h ago"
                  - generic [ref=e507]:
                    - button "Test" [ref=e508] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e509] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e510] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e511]:
              - generic [ref=e512]:
                - generic [ref=e513]:
                  - img [ref=e515]
                  - generic [ref=e520]:
                    - generic [ref=e521]:
                      - heading "e2ediag svc" [level=3] [ref=e522]
                      - generic [ref=e523]: rev 1
                      - generic "Retired" [ref=e524]:
                        - img [ref=e525]
                        - text: Retired
                    - paragraph [ref=e529]: HTTP service
                - generic "Healthy" [ref=e530]:
                  - img [ref=e531]
                  - text: Healthy
              - generic [ref=e534]:
                - generic [ref=e535]:
                  - generic [ref=e536]: Base URL
                  - generic [ref=e537]: https://dummyjson.com
                - generic [ref=e538]:
                  - generic [ref=e539]: Auth
                  - generic [ref=e540]: No auth
                - generic [ref=e541]:
                  - generic [ref=e542]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e543]: "Last tested: 1h ago"
                  - generic [ref=e544]:
                    - button "Test" [ref=e545] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e546] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e547] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e548]:
              - generic [ref=e549]:
                - generic [ref=e550]:
                  - img [ref=e552]
                  - generic [ref=e557]:
                    - generic [ref=e558]:
                      - heading "e2eui dummyjson" [level=3] [ref=e559]
                      - generic [ref=e560]: rev 1
                      - generic "Retired" [ref=e561]:
                        - img [ref=e562]
                        - text: Retired
                    - paragraph [ref=e566]: HTTP service
                - generic "Not Tested" [ref=e567]:
                  - img [ref=e568]
                  - text: Not Tested
              - generic [ref=e570]:
                - generic [ref=e571]:
                  - generic [ref=e572]: Base URL
                  - generic [ref=e573]: https://dummyjson.com
                - generic [ref=e574]:
                  - generic [ref=e575]: Auth
                  - generic [ref=e576]: No auth
                - generic [ref=e577]:
                  - generic [ref=e578]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e579]: "Last tested: never"
                  - generic [ref=e580]:
                    - button "Test" [ref=e581] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e582] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e583] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e584]:
              - generic [ref=e585]:
                - generic [ref=e586]:
                  - img [ref=e588]
                  - generic [ref=e593]:
                    - generic [ref=e594]:
                      - heading "e2eui dummyjson" [level=3] [ref=e595]
                      - generic [ref=e596]: rev 1
                      - generic "Retired" [ref=e597]:
                        - img [ref=e598]
                        - text: Retired
                    - paragraph [ref=e602]: HTTP service
                - generic "Not Tested" [ref=e603]:
                  - img [ref=e604]
                  - text: Not Tested
              - generic [ref=e606]:
                - generic [ref=e607]:
                  - generic [ref=e608]: Base URL
                  - generic [ref=e609]: https://dummyjson.com
                - generic [ref=e610]:
                  - generic [ref=e611]: Auth
                  - generic [ref=e612]: No auth
                - generic [ref=e613]:
                  - generic [ref=e614]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e615]: "Last tested: never"
                  - generic [ref=e616]:
                    - button "Test" [ref=e617] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e618] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e619] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e620]:
              - generic [ref=e621]:
                - generic [ref=e622]:
                  - img [ref=e624]
                  - generic [ref=e629]:
                    - generic [ref=e630]:
                      - heading "e2eui dummyjson" [level=3] [ref=e631]
                      - generic [ref=e632]: rev 1
                      - generic "Retired" [ref=e633]:
                        - img [ref=e634]
                        - text: Retired
                    - paragraph [ref=e638]: HTTP service
                - generic "Not Tested" [ref=e639]:
                  - img [ref=e640]
                  - text: Not Tested
              - generic [ref=e642]:
                - generic [ref=e643]:
                  - generic [ref=e644]: Base URL
                  - generic [ref=e645]: https://dummyjson.com
                - generic [ref=e646]:
                  - generic [ref=e647]: Auth
                  - generic [ref=e648]: No auth
                - generic [ref=e649]:
                  - generic [ref=e650]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e651]: "Last tested: never"
                  - generic [ref=e652]:
                    - button "Test" [ref=e653] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e654] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e655] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e656]:
              - generic [ref=e657]:
                - generic [ref=e658]:
                  - img [ref=e660]
                  - generic [ref=e665]:
                    - generic [ref=e666]:
                      - heading "e2eui dummyjson" [level=3] [ref=e667]
                      - generic [ref=e668]: rev 1
                      - generic "Retired" [ref=e669]:
                        - img [ref=e670]
                        - text: Retired
                    - paragraph [ref=e674]: HTTP service
                - generic "Not Tested" [ref=e675]:
                  - img [ref=e676]
                  - text: Not Tested
              - generic [ref=e678]:
                - generic [ref=e679]:
                  - generic [ref=e680]: Base URL
                  - generic [ref=e681]: https://dummyjson.com
                - generic [ref=e682]:
                  - generic [ref=e683]: Auth
                  - generic [ref=e684]: No auth
                - generic [ref=e685]:
                  - generic [ref=e686]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e687]: "Last tested: never"
                  - generic [ref=e688]:
                    - button "Test" [ref=e689] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e690] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e691] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e692]:
              - generic [ref=e693]:
                - generic [ref=e694]:
                  - img [ref=e696]
                  - generic [ref=e701]:
                    - generic [ref=e702]:
                      - heading "e2eui dummyjson" [level=3] [ref=e703]
                      - generic [ref=e704]: rev 1
                      - generic "Retired" [ref=e705]:
                        - img [ref=e706]
                        - text: Retired
                    - paragraph [ref=e710]: HTTP service
                - generic "Not Tested" [ref=e711]:
                  - img [ref=e712]
                  - text: Not Tested
              - generic [ref=e714]:
                - generic [ref=e715]:
                  - generic [ref=e716]: Base URL
                  - generic [ref=e717]: https://dummyjson.com
                - generic [ref=e718]:
                  - generic [ref=e719]: Auth
                  - generic [ref=e720]: No auth
                - generic [ref=e721]:
                  - generic [ref=e722]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e723]: "Last tested: never"
                  - generic [ref=e724]:
                    - button "Test" [ref=e725] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e726] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e727] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e728]:
              - generic [ref=e729]:
                - generic [ref=e730]:
                  - img [ref=e732]
                  - generic [ref=e737]:
                    - generic [ref=e738]:
                      - heading "e2eui dummyjson" [level=3] [ref=e739]
                      - generic [ref=e740]: rev 1
                      - generic "Retired" [ref=e741]:
                        - img [ref=e742]
                        - text: Retired
                    - paragraph [ref=e746]: HTTP service
                - generic "Not Tested" [ref=e747]:
                  - img [ref=e748]
                  - text: Not Tested
              - generic [ref=e750]:
                - generic [ref=e751]:
                  - generic [ref=e752]: Base URL
                  - generic [ref=e753]: https://dummyjson.com
                - generic [ref=e754]:
                  - generic [ref=e755]: Auth
                  - generic [ref=e756]: No auth
                - generic [ref=e757]:
                  - generic [ref=e758]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e759]: "Last tested: never"
                  - generic [ref=e760]:
                    - button "Test" [ref=e761] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e762] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e763] [cursor=pointer]:
                      - img
                      - text: Reinstate
        - generic [ref=e764]:
          - generic [ref=e765]:
            - img [ref=e767]
            - heading "Sink destination" [level=2] [ref=e770]
            - generic [ref=e771]: · 2 services — Endpoint + credentials the managed Connect sinks write to — selected by kafka+connect and kc blocks.
          - generic [ref=e772]:
            - generic [ref=e773]:
              - generic [ref=e774]:
                - generic [ref=e775]:
                  - img [ref=e777]
                  - generic [ref=e780]:
                    - generic [ref=e781]:
                      - heading "e2ea iceberg" [level=3] [ref=e782]
                      - generic [ref=e783]: rev 2
                      - generic "Retired" [ref=e784]:
                        - img [ref=e785]
                        - text: Retired
                    - paragraph [ref=e789]: Sink destination
                - generic "Healthy" [ref=e790]:
                  - img [ref=e791]
                  - text: Healthy
              - generic [ref=e794]:
                - generic [ref=e795]:
                  - generic [ref=e796]: Iceberg catalog
                  - generic [ref=e797]: https://polaris.datapasc.com/api/catalog
                - generic [ref=e798]:
                  - generic [ref=e799]: Warehouse
                  - generic [ref=e800]: bronze
                - generic [ref=e801]:
                  - generic [ref=e802]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e803]: "Last tested: 8h ago"
                  - generic [ref=e804]:
                    - button "Test" [ref=e805] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e806] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e807] [cursor=pointer]:
                      - img
                      - text: Reinstate
            - generic [ref=e808]:
              - generic [ref=e809]:
                - generic [ref=e810]:
                  - img [ref=e812]
                  - generic [ref=e815]:
                    - generic [ref=e816]:
                      - heading "e2er iceberg" [level=3] [ref=e817]
                      - generic [ref=e818]: rev 1
                      - generic "Retired" [ref=e819]:
                        - img [ref=e820]
                        - text: Retired
                    - paragraph [ref=e824]: Sink destination
                - generic "Healthy" [ref=e825]:
                  - img [ref=e826]
                  - text: Healthy
              - generic [ref=e829]:
                - generic [ref=e830]:
                  - generic [ref=e831]: Iceberg catalog
                  - generic [ref=e832]: https://polaris.datapasc.com/api/catalog
                - generic [ref=e833]:
                  - generic [ref=e834]: Warehouse
                  - generic [ref=e835]: bronze
                - generic [ref=e836]:
                  - generic [ref=e837]:
                    - button "0 dependent flows" [disabled]:
                      - img
                      - text: 0 dependent flows
                    - generic [ref=e838]: "Last tested: 3h ago"
                  - generic [ref=e839]:
                    - button "Test" [ref=e840] [cursor=pointer]:
                      - img
                      - text: Test
                    - button "Edit" [ref=e841] [cursor=pointer]:
                      - img
                      - text: Edit
                    - button "Reinstate" [ref=e842] [cursor=pointer]:
                      - img
                      - text: Reinstate
```

# Test source

```ts
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
  321 |   await page.getByRole("option", { name: TOPIC }).click();
  322 |   await expect(page.getByText(/offset \d+/).first()).toBeVisible({ timeout: 30_000 });
  323 |   await shot("07b-messages-ui");
  324 |   await page.keyboard.press("Escape"); // close the sheet
  325 | });
  326 | 
  327 | // ---------------------------------------------------------------- Step 8
  328 | test("step08 REGRESSION stop -> undeploy -> redeploy passes preflight (own topic not a collision)", async () => {
  329 |   test.setTimeout(600_000);
  330 |   await page.goto(`/flow-builder/${flowId}`);
  331 | 
  332 |   await page.getByRole("button", { name: "Stop", exact: true }).click();
  333 |   await expect(page.getByText("Stopped — queues retained")).toBeVisible({ timeout: 120_000 });
  334 |   await shot("08-stopped");
  335 | 
  336 |   await page.getByRole("button", { name: "More" }).click();
  337 |   await page.getByRole("menuitem", { name: "Undeploy", exact: true }).click();
  338 |   await expect(page.getByText(/Undeployed — generated topics emptied/)).toBeVisible({ timeout: 180_000 });
  339 |   await shot("08b-undeployed");
  340 | 
  341 |   // Deploy again — the user's bug: blocked by the flow's OWN leftover topic.
  342 |   await page.getByRole("button", { name: "Deploy", exact: true }).click();
  343 |   const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  344 |   await expect(dlg).toBeVisible();
  345 |   await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  346 |   await expect(dlg.locator("svg.text-destructive")).toHaveCount(0);
  347 |   await expect(dlg.getByText(/not owned by this flow/)).toHaveCount(0);
  348 |   await shot("08c-preflight-2nd");
  349 | 
  350 |   const deployBtn = dlg.getByRole("button", { name: "Deploy" });
  351 |   await expect(deployBtn).toBeEnabled();
  352 |   await deployBtn.click();
  353 | 
  354 |   await expect(page.getByText("Deployed — the flow is built stopped")).toBeVisible({ timeout: 240_000 });
  355 |   await expect(page.getByText(/not owned by this flow/)).toHaveCount(0);
  356 |   await expect(page.getByText(/Deploy failed/)).toHaveCount(0);
  357 |   await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });
  358 |   await shot("08d-redeployed");
  359 | });
  360 | 
  361 | // ---------------------------------------------------------------- Step 9
  362 | test("step09 UI cleanup — delete flow, retire service", async () => {
  363 |   test.setTimeout(600_000);
  364 | 
  365 |   // Delete requires undeployed: undeploy the just-redeployed flow first (UI).
  366 |   await page.getByRole("button", { name: "More" }).click();
  367 |   await page.getByRole("menuitem", { name: "Undeploy", exact: true }).click();
  368 |   await expect(page.getByText(/Undeployed — generated topics emptied/)).toBeVisible({ timeout: 180_000 });
  369 | 
  370 |   await page.goto("/flows");
  371 |   await page.getByPlaceholder("Search flows, entities, topics…").fill("e2eui");
  372 |   const row = page.getByRole("row").filter({ hasText: FLOW_NAME });
  373 |   await expect(row).toBeVisible();
  374 |   await row.getByRole("button", { name: "More actions" }).click();
  375 |   await page.getByRole("menuitem", { name: "Delete" }).click();
  376 | 
  377 |   const delDlg = page.getByRole("alertdialog");
  378 |   await expect(delDlg.getByText(`Delete "${FLOW_NAME}"?`)).toBeVisible();
  379 |   await delDlg.getByRole("button", { name: "Delete flow" }).click();
  380 |   await expect(page.getByText(`Deleted — ${FLOW_NAME}`)).toBeVisible({ timeout: 120_000 });
  381 |   await expect(page.getByRole("row").filter({ hasText: FLOW_NAME })).toHaveCount(0, { timeout: 30_000 });
  382 |   await shot("09-flow-deleted");
  383 | 
  384 |   // Retire the service. NOTE: retirement is logical by design ("no hard
  385 |   // delete") — the card stays listed, marked Retired, with Reinstate.
  386 |   await page.goto("/application-services");
  387 |   const activeCard = page
  388 |     .locator("div.bg-card")
  389 |     .filter({ hasText: SERVICE_NAME })
  390 |     .filter({ hasNotText: "Retired" })
  391 |     .first();
  392 |   await expect(activeCard).toBeVisible();
  393 |   await activeCard.getByRole("button", { name: "Retire" }).click();
  394 | 
  395 |   const retDlg = page.getByRole("alertdialog");
  396 |   await expect(retDlg.getByText(`Retire "${SERVICE_NAME}"?`)).toBeVisible();
  397 |   await retDlg.getByRole("button", { name: "Retire Service" }).click();
  398 |   await expect(page.getByText(`"${SERVICE_NAME}" retired`)).toBeVisible({ timeout: 60_000 });
  399 | 
  400 |   // No ACTIVE card with that name remains; the retired card shows Reinstate.
  401 |   await expect(
  402 |     page.locator("div.bg-card").filter({ hasText: SERVICE_NAME }).filter({ hasNotText: "Retired" }),
  403 |   ).toHaveCount(0, { timeout: 30_000 });
  404 |   const retiredCard = page.locator("div.bg-card").filter({ hasText: SERVICE_NAME }).first();
> 405 |   await expect(retiredCard.getByRole("button", { name: "Reinstate" })).toBeVisible();
      |                                                                        ^ Error: expect(locator).toBeVisible() failed
  406 |   await shot("09b-service-retired");
  407 | });
  408 | 
  409 | // ---------------------------------------------------------------- Step 10
  410 | test("step10 post-cleanup API sweep — no e2eui flow remains", async () => {
  411 |   const r = await fetch(`${BACKEND}/api/v2/flows/`);
  412 |   expect(r.ok).toBe(true);
  413 |   const flows = (await r.json()) as { id: string; name?: string }[];
  414 |   const leftovers = flows.filter((f) => String(f.name ?? "").toLowerCase().startsWith("e2eui"));
  415 |   expect(leftovers, `e2eui flows left on the backend: ${JSON.stringify(leftovers.map((f) => f.name))}`).toEqual([]);
  416 |   // NiFi PG removal is the backend delete's duty; the empty flows list is the
  417 |   // observable contract asserted here (best-effort per instructions).
  418 |   await shot("10-final-flows-list");
  419 | });
  420 | 
```