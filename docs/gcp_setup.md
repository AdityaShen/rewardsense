# GCP Setup Guide for RewardSense

## Project Information

- **Project Name:** rewardsense
- **Project ID:** rewardsense
- **Region:** us-central1
- **Billing:** Northeastern University Google Cloud Account

---

## Resources Created

### Cloud Storage Bucket

- **Name:** `gs://rewardsense-dvc-store`
- **Purpose:** DVC remote storage for data versioning
- **Location:** us-central1 (single region)
- **Storage Class:** Standard
- **Access Control:** Uniform
- **Public Access:** Prevented (private only)
- **Data Protection:** Soft delete enabled (7-day retention)

### Service Account

- **Name:** rewardsense-pipeline
- **Email:** rewardsense-pipeline@rewardsense.iam.gserviceaccount.com
- **Roles Assigned:**
  - Storage Object Admin (for DVC bucket operations)
  - BigQuery Data Editor (for future analytics)
  - Cloud SQL Client (for future database access)

> **Note:** Service account JSON key creation is disabled by organization policy. Team members authenticate using personal credentials instead (see below).

---

## Team Member Setup Instructions

### Prerequisites

- Google account with access to the RewardSense GCP project
- macOS, Linux, or Windows machine

### Step 1: Get Project Access

Contact the project admin to be added to the GCP project with "Editor" role.

### Step 2: Install gcloud CLI

**macOS (recommended method):**
```bash
# Download from: https://cloud.google.com/sdk/docs/install
# Extract and run:
./google-cloud-sdk/install.sh

# Restart terminal or run:
source ~/.zshrc
```

**Alternative (Homebrew):**
```bash
brew install --cask google-cloud-sdk
```

**Verify installation:**
```bash
gcloud --version
```

### Step 3: Initialize gcloud

```bash
gcloud init
```

This will:
1. Open browser for Google account sign-in
2. Ask you to select a project → Choose `rewardsense`
3. Configure default settings

### Step 4: Set Up Application Default Credentials

Since service account keys are disabled by organization policy, authenticate using your personal Google credentials:

```bash
gcloud auth application-default login
```

This opens a browser — sign in with the same Google account that has project access. This creates credentials that DVC and other tools will use automatically.

### Step 5: Verify Access

Test that you can access the storage bucket:

```bash
gsutil ls gs://rewardsense-dvc-store
```

**Expected output:** Empty (no files yet) or list of files — no errors.

If you see a "permission denied" error, contact the project admin to verify your IAM permissions.

---

## Enabling APIs

GCP may prompt you to enable APIs when running certain commands. When asked:

```
API [some-api.googleapis.com] not enabled on project [rewardsense]. 
Would you like to enable and retry? (y/N)
```

Type `y` and press Enter. This is normal and doesn't incur charges for basic APIs.

---

## Quick Reference Commands

```bash
# Check current configuration
gcloud config list

# Switch projects (if you have multiple)
gcloud config set project rewardsense

# List bucket contents
gsutil ls gs://rewardsense-dvc-store

# Re-authenticate if credentials expire
gcloud auth login
gcloud auth application-default login
```

---

## Troubleshooting

### "Permission denied" on ~/.config folder (macOS)
```bash
sudo chown -R $(whoami) ~/.config
```

### "command not found: gcloud"
```bash
source ~/.zshrc   # or restart terminal
```

### "Permission denied" accessing bucket
- Verify you're logged into the correct Google account: `gcloud auth list`
- Contact project admin to check IAM permissions

---

## Cost Information

| Resource | Cost | Notes |
|----------|------|-------|
| Cloud Storage bucket | ~$0.02/GB/month | Only pay for data stored |
| Empty bucket | $0 | No charge until data is uploaded |
| API calls | Negligible | Free tier covers typical usage |

---

## Project Admin Tasks

### Adding New Team Members

1. Go to GCP Console → IAM & Admin → IAM
2. Click "Grant Access"
3. Enter team member's Google email
4. Assign role: `Editor`
5. Click Save

### Resources to Create Later (Not Needed for Phase 1)

- Cloud SQL PostgreSQL instance (for production Airflow metadata)
- BigQuery datasets (for analytics)

---

*Last updated: February 2025*
*Setup completed by: Arjun Avadhani*