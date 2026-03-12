# MTD Funnel Reporting Dashboard

Month-to-date booked call tracker by marketing funnel, powered by Close CRM.

**Live dashboard:** [https://stephenolivas.github.io/funnel-reporting-mtd/](https://stephenolivas.github.io/funnel-reporting-mtd/)

## Architecture

```
Close CRM API → fetch_data.py → data.json → index.html (GitHub Pages)
                                          └→ archives/
```

- **cron-job.org** triggers a `workflow_dispatch` every 15 min, Mon–Fri, 7 AM–5 PM Pacific
- **GitHub Actions** runs `fetch_data.py`, commits `data.json` + archives, pushes to `main`
- **GitHub Pages** serves `index.html` which reads `data.json` at load time

## Setup

### 1. Create the GitHub repo

```bash
git init funnel-reporting-mtd
cd funnel-reporting-mtd
# copy all files in, then:
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/stephenolivas/funnel-reporting-mtd.git
git push -u origin main
```

### 2. Enable GitHub Pages

Repo → Settings → Pages → Source: **Deploy from a branch** → Branch: `main` / `/ (root)`

### 3. Add the Close API key secret

Repo → Settings → Secrets and variables → Actions → New repository secret:
- **Name:** `CLOSE_API_KEY`
- **Value:** your Close API key

### 4. Test the workflow

Repo → Actions → "Update MTD Funnel Dashboard" → **Run workflow**

Watch the logs — you should see meeting pagination, classification counts, and lead fetch progress.

### 5. Set up cron-job.org

Create a new cron job:
- **URL:** `https://api.github.com/repos/stephenolivas/funnel-reporting-mtd/actions/workflows/update-dashboard.yml/dispatches`
- **Method:** POST
- **Headers:**
  - `Authorization: token YOUR_GITHUB_PAT`
  - `Accept: application/vnd.github.v3+json`
  - `Content-Type: application/json`
- **Body:** `{"ref":"main"}`
- **Schedule:** Every 15 minutes, Mon–Fri, 7 AM–5 PM Pacific

> GitHub PAT needs `workflow` scope. Create at GitHub → Settings → Developer settings → Personal access tokens.

## Updating Monthly Goals

Edit `goals.json` in the repo root — no Python changes needed:

```json
{
  "goals": {
    "Low Ticket Funnel": 400,
    "Instagram": 240,
    "VSL": 200
  }
}
```

Commit and push. The next run will pick up the new goals.

## File Structure

```
funnel-reporting-mtd/
├── index.html                  ← Live dashboard (reads data.json)
├── archive.html                ← Archive viewer
├── data.json                   ← Current MTD data (overwritten each run)
├── goals.json                  ← Monthly goals (edit here monthly)
├── archives/
│   ├── index.json              ← List of available snapshots
│   ├── data_week_YYYY-MM-DD.json
│   └── data_month_YYYY-MM.json
├── scripts/
│   └── fetch_data.py           ← Data fetcher
└── .github/workflows/
    └── update-dashboard.yml    ← GitHub Actions workflow
```

## Classification Logic

Pipeline (order is critical for performance):
1. Paginate ALL meetings (~107 API calls — Close ignores date filters)
2. Filter to MTD range in Python (UTC → Pacific conversion)
3. Classify titles in Python (zero API calls)
4. Collect unique `lead_id`s from surviving meetings
5. Fetch only those leads (~80–200 API calls)
6. Apply lead-level exclusions at fetch time

See `scripts/fetch_data.py` for full title classification rules and hard exclusion lists.

## Performance Budget

| Step | API Calls | Notes |
|------|-----------|-------|
| Meeting pagination | ~107 | 0.5s throttle per call |
| Lead fetches | ~80–200 | Only classified meetings |
| **Total** | **~190–310** | **2–4 min runtime** |

GitHub Actions timeout: 20 minutes (generous safety buffer).
