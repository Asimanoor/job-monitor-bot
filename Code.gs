/**
 * Job Bot — Google Apps Script
 * ────────────────────────────
 * Adds a custom "Job Bot" menu to the Google Sheet with:
 *   • Mark Selected as Applied
 *   • Open Apply Link
 *   • Archive Old Jobs
 *
 * SETUP:
 *   1. In Google Sheets → Extensions → Apps Script
 *   2. Delete any existing code in Code.gs
 *   3. Paste this entire file
 *   4. Click Save (Ctrl+S)
 *   5. Reload the spreadsheet
 *   6. Accept permissions when prompted
 *   7. The "Job Bot" menu appears in the menu bar
 */

// ── Create menu on open ─────────────────────────────────────────────────────
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('🤖 Job Bot')
    .addItem('✅ Mark Selected as Applied', 'markAsApplied')
    .addItem('🔗 Open Apply Link', 'openApplyLink')
    .addSeparator()
    .addItem('📊 Show Dashboard Stats', 'showDashboardStats')
    .addToUi();
}


// ── Mark selected row(s) as Applied ─────────────────────────────────────────
function markAsApplied() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Sheet1');
  var selection = sheet.getActiveRange();
  var startRow = selection.getRow();
  var numRows = selection.getNumRows();

  if (startRow <= 1) {
    SpreadsheetApp.getUi().alert('⚠️ Select a job row first (not the header).');
    return;
  }

  var count = 0;
  for (var i = 0; i < numRows; i++) {
    var row = startRow + i;
    if (row <= 1) continue;

    // Column J (10) = Status
    sheet.getRange(row, 10).setValue('Applied');

    // Column K (11) = Notes
    var existingNotes = sheet.getRange(row, 11).getValue();
    var note = 'Applied via sheet — ' + new Date().toLocaleString();
    if (existingNotes) {
      note = existingNotes + ' | ' + note;
    }
    sheet.getRange(row, 11).setValue(note);

    // Highlight row light green
    sheet.getRange(row, 1, 1, 11).setBackground('#d9ead3');

    count++;
  }

  SpreadsheetApp.getActiveSpreadsheet().toast(
    count + ' row(s) marked as Applied ✅',
    'Job Bot',
    3
  );
}


// ── Open Apply Link in new tab ──────────────────────────────────────────────
function openApplyLink() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Sheet1');
  var row = sheet.getActiveCell().getRow();

  if (row <= 1) {
    SpreadsheetApp.getUi().alert('⚠️ Select a job row first.');
    return;
  }

  // Column G (7) = Apply Link
  var link = sheet.getRange(row, 7).getValue();

  if (!link) {
    SpreadsheetApp.getUi().alert('⚠️ No apply link found in this row.');
    return;
  }

  // HTML popup to open the link (Apps Script can't directly open URLs)
  var html = HtmlService
    .createHtmlOutput(
      '<script>window.open("' + link + '", "_blank");google.script.host.close();</script>'
    )
    .setWidth(1)
    .setHeight(1);

  SpreadsheetApp.getUi().showModelessDialog(html, 'Opening link…');
}


// ── Show Dashboard Stats ────────────────────────────────────────────────────
function showDashboardStats() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Sheet1');
  var data = sheet.getDataRange().getValues();

  var stats = {
    total: data.length - 1,
    new_count: 0,
    applied: 0,
    interviewing: 0,
    offer: 0,
    rejected: 0
  };

  for (var i = 1; i < data.length; i++) {
    var status = (data[i][9] || '').toString().trim().toLowerCase();
    switch (status) {
      case 'new':          stats.new_count++;    break;
      case 'applied':      stats.applied++;      break;
      case 'interviewing': stats.interviewing++; break;
      case 'offer':        stats.offer++;        break;
      case 'rejected':     stats.rejected++;     break;
    }
  }

  var msg =
    '📊 Dashboard Stats\n\n' +
    'Total Jobs: ' + stats.total + '\n' +
    '🆕 New: ' + stats.new_count + '\n' +
    '✅ Applied: ' + stats.applied + '\n' +
    '🎤 Interviewing: ' + stats.interviewing + '\n' +
    '🎉 Offers: ' + stats.offer + '\n' +
    '❌ Rejected: ' + stats.rejected;

  SpreadsheetApp.getUi().alert(msg);
}
