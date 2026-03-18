# Google Sheet Setup Guide

This guide defines the expected sheet structure used by `google_sheets_client.py`, `seed_sheet_data.py`, and `test_notifications.py`.

## Required Header Row (A1:L1)

Use this exact order:

1. `Timestamp`
2. `Job Title`
3. `Company`
4. `Location`
5. `Job Type`
6. `Posted Date`
7. `Apply Link`
8. `Description`
9. `Matched Keywords`
10. `Status`
11. `Notes`
12. `AI_Score`

> If the sheet is empty, the app auto-creates this header row.

## Recommended Conditional Formatting

Apply these rules to range `A2:L`:

1. **Applied** rows (light green)
   - Custom formula: `=$J2="Applied"`
   - Background: `#d9ead3`

2. **Rejected** rows (light red)
   - Custom formula: `=$J2="Rejected"`
   - Background: `#f4cccc`

3. **Interviewing** rows (light blue)
   - Custom formula: `=$J2="Interviewing"`
   - Background: `#d9e8fb`

4. **Low AI score warning** (light amber)
   - Apply to column `L2:L`
   - Custom formula: `=AND($L2<>"",$L2<70)`
   - Background: `#fff2cc`

## Suggested Filters

Create filter views for quick triage:

- Pending Applications (`Status = New`)
- Applied Jobs (`Status = Applied`)
- Interviews (`Status = Interviewing`)
- High Match (`AI_Score >= 85`)

## Notes

- `Apply Link` should be unique per job.
- `Status` is updated by workflow/manual review.
- `AI_Score` is written by the AI-enhanced filter pipeline.
