# 015 — Deterministic JSON field order and CSV header order

**Status: SPEC. The implementation lands as a new candidate; C1/C2 must be re-run under
new execution identities before anything here is evidence.**

**This changes `api/` behaviour.** It does not change values, row order, HTTP statuses,
error bodies, or any query semantic.

---

## 1. What was found, and how

`pm2G` (2026-08-24, `PM2-staging-result-pm2G.md` §7) restarted the staging candidate and
compared the response to the one taken before the restart. Same 144 rows, same values,
**different column order**:

| | before restart (pid 1455874) | after restart (pid 1456369) |
|---|---|---|
| CSV header | `lon,lat,depth,time_period,`**`temperature_an,temperature`** | `lon,lat,depth,time_period,`**`temperature,temperature_an`** |
| CSV row 1 | `134.5,14.5,0.0,0,`**`0.0,0.5`** | `134.5,14.5,0.0,0,`**`0.5,0.0`** |
| JSON first object keys | `…,`**`temperature_an, temperature`** | `…,`**`temperature, temperature_an`** |

The JSON rows compare **equal as objects** — only the serialised key order differs. The
CSV differs **as text**, and the value columns move with the header, so a positional CSV
consumer reads `temperature_an` where it previously read `temperature`.

**Characterisation, from `scratchpad/pm2G/08-column-order.txt`:**

| probe | result |
|---|---|
| 8 identical requests to one process (CSV header) | **identical all 8** |
| 8 identical requests to one process (JSON key order) | **identical all 8** |
| `append=an,mn` vs `append=mn,an` on one process | **same order both ways** |
| across a process restart | **CHANGED** |

**Stable within a process, different between processes, independent of the request.**
That is iteration over a hash-seeded container, fixed per interpreter by `PYTHONHASHSEED`.

## 2. Why this was not caught before

**It was deferred deliberately, and the deferral is in the source.** `api/query.py` ends
with:

> "Row order only. Column order still follows the `list(set(...))` calls above and is
> deliberately untouched (spec 008 section 3)."

And [008 §3](008-s2b-deterministic-row-order.md) puts it out of scope in as many words:

> "**JSON field order and CSV header order.** Not changed by this work. The
> `list(set(...))` over `variables` still drives column order, and it stays exactly as
> it is."

**That was a correct scope decision for 008 and it is not being criticised here.** What
it did not anticipate is that the OpenAPI 1.1.0 description, written alongside it, says:

> "Row order (since 1.1.0): … **JSON field order and CSV header order are unchanged.**"

**008 meant "unchanged *by this work*". A consumer reads "unchanged" as "stable".** Those
are different claims, and only the first was true. `c1f`/`c2g` compared responses within
a process and so could not see it; `pm2G` restarted the process and did.

**The observed variable-major order (`temperature_an` before `temperature`, or the
reverse) was never a contract.** It was whatever the hash seed produced that run. Neither
observation may be back-filled as intended behaviour.

## 3. Where the nondeterminism comes from

Five unordered containers feed column order, all in `api/query.py`:

| line | expression | what it feeds |
|---|---|---|
| 121 | `variables = list(set(...))` | which `{param}_{var}` columns exist, and the order `result_list` is built in |
| 132 | `pars = list(set(...))` | requested parameters |
| 141 | `periods = list(set(...))` | **already `.sort()`ed — not a source** |
| 149 | `zarr_group_paths = set()` | the order groups are opened, hence the order frames are concatenated |
| 198 / 205 | `existing_params.intersection(pars)` → `list(selected_params)` | per-group parameter order |

`pivot(on="parameter_variable")` then emits columns in **order of first appearance** in
the concatenated frame, so every one of the above reaches the output.

## 4. The canonical order — decided, not observed

**Decision (PI, 2026-08-25): parameter-major.**

1. **Index columns, fixed and first:** `lon, lat, depth, time_period`.
2. **Data columns follow the canonical `available_pars` order**, restricted to the
   parameters actually present.
3. **Within each parameter, the canonical `available_vars` order**, restricted to the
   statistics actually requested and present.
4. **`mn` is renamed to the bare parameter name but keeps the `mn` slot** — the rename
   changes the label, never the position.
5. **Independent of** the query's `parameter` / `append` input order, set iteration,
   Zarr group open order, and `PYTHONHASHSEED`.
6. **JSON field order and CSV header order are identical** and stable across processes
   and restarts.

The canonical sequences, as declared today:

```
available_pars (1°)    : temperature, salinity, oxygen, o2sat, AOU,
                         silicate, phosphate, nitrate
available_pars (0.25°) : temperature, salinity
available_vars         : an, mn, dd, ma, sd, se, oa, gp, sdo, sea
```

`an` precedes `mn` because `available_vars` declares it so — **not** because a request
listed it first.

**Worked example.** `parameter=temperature,salinity,oxygen&append=mn,an` — note the
request lists `mn` first and `oxygen` last; neither affects the result:

```
lon,lat,depth,time_period,temperature_an,temperature,salinity_an,salinity,oxygen_an,oxygen
```

**Why parameter-major.** Column names are `{param}_{var}`, so the parameter is the
leading key in the name; the public documentation lists parameters as the primary axis;
and a consumer pulling one parameter's statistics gets them contiguous. Variable-major
was considered and rejected — see §4.1.

### 4.1 What the existing evidence did and did not settle

Every recorded header in the tree carries **one** data column
(`lon,lat,depth,time_period,nitrate` in [spec 006](006-json-csv-empty-result-consistency.md),
`lon,lat,depth,time_period,t` in `bench/test_contract.py`). Those pin the **index
columns** and nothing else: with a single data column, parameter-major and
variable-major are identical. **The nesting was genuinely undetermined by the evidence
and was decided by the PI, not inferred.**

## 5. Design

**One explicit ordered `select`, once, immediately before the return** — after the rename
and after the row sort, where both endpoints already share the frame.

- **One site, not two.** `api/app.py`'s JSON path calls `to_dicts()` on this frame and
  the CSV path calls `write_csv()`, so a single `select` governs both and the rule is
  not written twice. This is the same argument 008 §5.1 used for the sort site.
- **Last, so nothing downstream reintroduces the instability** — every unordered input
  has already had its say by then.
- **A `select`, not a sequence of moves.** One pass, one materialisation; no repeated
  shuffling of data.
- **Names, not positions.** The projection is built from the frame's own column names,
  so it cannot pair a header with the wrong values — the failure mode that makes this
  bug harmful in CSV.
- **Every column is accounted for.** The projection is asserted to be a permutation of
  the frame's existing columns: same set, same length. A column that exists and is not
  placed is a bug, and dropping data silently is worse than the bug being fixed.

The canonical order is computed by a named helper so the tests can call it directly
and the ordering rule has one definition rather than being inlined at the call site.

## 6. What is NOT changed

- **Values.** Identical, cell for cell.
- **Row order.** The 008 contract stands unchanged: ascending by
  `(time_period numeric, depth, lat, lon)`.
- **Column names.** Including the `{param}_mn` → `{param}` rename.
- **HTTP statuses**, error bodies, and the 400/404 paths.
- **Empty-result behaviour.** Spec 006 option B remains unimplemented; this work does
  not implement it and does not depend on it.
- **Request parameter meaning.**
- **`api/app.py`.** The change is confined to `api/query.py`.

## 7. Tests

Added to the offline suite. The suite must fail if the canonical ordering is removed.

| test | what it pins |
|---|---|
| multiple independent processes | column order identical across separate interpreters |
| multiple `PYTHONHASHSEED` values | order does not follow the hash seed |
| before / after a restart | the pm2G case, offline |
| JSON field sequence | exact expected sequence, not just "stable" |
| CSV header sequence | identical to the JSON field sequence |
| `append` input order permuted | `an,mn` and `mn,an` give the same column order |
| `parameter` input order permuted | likewise |
| multi-group / multi-variable / `mn` / `an` | groups opened in any order give one output order |
| normal / empty / error responses | statuses and bodies unchanged |
| values and row order | unchanged against the pre-change output |
| **negative control** | with the canonical ordering removed, the suite **fails** |

The hash-seed and multi-process tests run the query in **subprocesses with explicit
`PYTHONHASHSEED` values**, because a single interpreter cannot observe its own seed
varying — which is precisely why the offline suite missed this and a restart on VM24
found it.

## 8. Consequences for existing evidence

**This modifies `api/`, so:**

- a **new candidate commit** is required;
- **C1 and C2 must be re-run under new execution identities**;
- **`c1f`, `c2g`, `s2pB` and `pm2G` evidence must NOT be back-filled** — none of them
  ran this tree;
- **the old `s2pB` latency result does not apply to the new API tree.** If the cost of
  the ordered `select` needs measuring, that is a **separate measurement** with its own
  authorisation; the old figure may not be cited for it.

**`pm2G` remains NOT A PASS** and is not re-classified by this work. Its identity and
port `18265` remain consumed, and its state remains retained on VM24.

## 9. Open, and deliberately not resolved here

**The OpenAPI wording.** The 1.1.0 description's "JSON field order and CSV header order
are unchanged" becomes *true as a stability claim* once this lands. Whether to reword it
to say so positively — and whether that is a 1.1.1 — is a **documentation decision for
the PI**, not something this spec takes. No wording is changed by this work.
