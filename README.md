# North Lake Dispatch

Field-operations platform for Tahoe Getaways: vehicle routing, Breezeway task
orchestration, and AI-assisted daily operations reporting.

Flask service backed by PostgreSQL, deployed via Gunicorn.

---

## Architecture

```
northlakedispatch/
├── app.py                  # Entrypoint — blueprint registration, Flask-Login, APScheduler
├── db.py                   # Connection helpers, User model, shared operational config
├── routes/                 # 26 feature blueprints (one bounded context each)
├── templates/              # Jinja2 views
├── static/                 # Assets + compiled Tailwind bundle
├── tools/                  # Standalone batch/report generators (run out-of-band)
├── reports/                # Generated report artifacts (gitignored; read by the app)
├── requirements.txt
├── package.json            # Tailwind build toolchain
└── Procfile                # gunicorn app:app
```

**Persistence** is PostgreSQL, addressed through `DATABASE_URL`. There is no local
database file and no CSV import step — property data lives in the `properties`
table and is administered through the in-app admin UI.

---

## Setup

Requires Python 3.13.13 (pinned in `.python-version`) and Node 20+.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
npm install
npm run build:css        # compiles tailwind.css → static/tailwind.min.css
```

Configuration is supplied entirely through environment variables — locally via a
`.env` file (loaded by `python-dotenv`), and in production through the host's
secret store. No credentials are committed to this repository.

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | PostgreSQL connection string |
| `SECRET_KEY` | Flask session signing key |
| `BREEZEWAY_CLIENT_ID` / `BREEZEWAY_CLIENT_SECRET` | Breezeway API credentials |
| `BW_WEBHOOK_SECRET` | Shared secret validating inbound Breezeway webhooks |
| `ANTHROPIC_API_KEY` | Claude API access for the briefing and assistants |
| `GOOGLE_MAPS_API_KEY` | Distance Matrix + geocoding |
| `ASANA_TOKEN` | Asana integration |
| `CRON_SECRET` | Bearer token authorizing unauthenticated cron endpoints |
| `PRIMARY_ADMIN_EMAIL` / `APP_ADMIN_PW` | Bootstrap admin credentials |
| `SECURITY_PAGE_PIN` | Gate for the security settings page |
| `APP_BASE_URL` | Absolute base URL for generated links |
| `PORT` | Bind port (supplied by the platform) |

Run locally:

```bash
python app.py            # dev server on $PORT (default 5000)
gunicorn app:app         # production-equivalent
```

---

## Features

### Routing & dispatch
- **Constraint-programming route optimization** — OR-Tools `RoutingModel` over a
  Google Distance Matrix cost matrix, using guided local search as the
  metaheuristic with cheapest-arc/cheapest-insertion first-solution strategies.
  Supports open-ended routes with an arbitrary terminal depot.
- **Route persistence and sharing** — named saved routes with tokenized public
  viewer links for field staff without accounts.
- **Mobile self-service routing** — cleaners submit their own stop list and
  receive an optimized ordering as a deep link into Google Maps.
- **Geospatial property catalog** — geocoded portfolio with map rendering and a
  backfill path for un-geocoded records.

### Breezeway integration
- **Bidirectional task sync** — writes optimized route ETAs back to existing
  Breezeway tasks as scheduled start times, with a strict no-create invariant so
  the platform can never author tasks upstream.
- **Webhook ingestion** — shared-secret-verified inbound webhooks parse task
  comments against configurable mention rules and fan out per-user alerts.
- **Concurrent scan fan-out** — `ThreadPoolExecutor`-parallelized API sweeps
  keep multi-hundred-request audits inside request timeouts.
- **Batch assignment** — date-scoped task assignment bucketed by property group.

### Compliance & exception monitoring
- **Post-Reservation Inspection (PRI) scanner** — read-through, cache-free
  detection of missing inspections, with partial-response guarding so an
  incomplete upstream page cannot manufacture false vacancies.
- **Hot tub service SLA tracking** — 45-day lookback flagging services past a
  14-day interval, plus proximity detection for probable double-bookings.
  Manual acknowledgements auto-expire on a 24-hour TTL.
- **Occupancy conflict detection** — surfaces work scheduled at properties while
  guests or long-term tenants are in residence.
- **Off-roster assignee monitoring** — passive daily sweep flagging assignments
  to anyone outside an operator-maintained allowlist, regardless of origin.
- **Bear fence date reconciliation** — cross-references walk-through tasks
  against their paired disarm tasks and proposes date alignment.
- **Lease arrival prep audit** — 30-day forward scan of long-stay arrivals with
  full preceding task history per reservation.
- **Seasonal inspection tracking** — per-property completion state for annual
  inspection campaigns.

### AI-assisted operations
- **Generated daily briefing** — Claude synthesizes saved routes, arrivals, and
  departures into a plain-language operations summary, memoized per date on a
  15-minute TTL to bound API spend.
- **Operations assistant** — admin-scoped chat over operational context with
  server-sent-event streaming and persisted session history.
- **Personal assistant with Asana integration** — task triage isolated from the
  operations assistant so failures cannot cascade between them.

### Platform
- **Modular blueprint decomposition** — 26 independently registered blueprints,
  deliberately share-nothing so feature work is blast-radius-limited.
- **Session auth with role-based access control** — Flask-Login with a
  decorator-enforced admin boundary, invite-based registration, and
  expiring password-reset tokens.
- **Scheduled background jobs** — APScheduler running a timezone-pinned daily
  PRI scan and a 30-minute Asana poll in-process.
- **Token-authorized cron endpoints** — bearer-authenticated hooks for external
  schedulers, bypassing session auth without weakening it.
- **Out-of-band report generation** — expensive analytics run as standalone
  programs writing JSON artifacts; the web tier only presents them, keeping
  slow batch work off the request path.
