' Starts WealthPipeline advisor app with no window (runs via cmd).
' Use "Add WealthPipeline to Windows Startup.bat" in this folder.

Option Explicit

Dim sh, fso, winDir, scriptsDir, root, scriptPy, cmd

Set fso = CreateObject("Scripting.FileSystemObject")
winDir = fso.GetParentFolderName(WScript.ScriptFullName)
scriptsDir = fso.GetParentFolderName(winDir)
root = fso.GetParentFolderName(scriptsDir)

If Not fso.FolderExists(root & "\wealth_leads") Then
    MsgBox "Could not find wealth_leads folder (expected at: " & root & ")", vbCritical, "WealthPipeline"
    WScript.Quit 1
End If

If Not fso.FileExists(root & "\serve_advisor.py") Then
    MsgBox "Missing serve_advisor.py at project root: " & root, vbCritical, "WealthPipeline"
    WScript.Quit 1
End If

Set sh = CreateObject("WScript.Shell")

' Exit only if the FastAPI advisor is already up (healthz + header). If another app
' owns 8765 (e.g. legacy "wealth_leads serve"), the script frees the port so we can start.
Dim ps, rc, ensureScript
ensureScript = winDir & "\ensure_advisor_on_8765.ps1"
If Not fso.FileExists(ensureScript) Then
    MsgBox "Missing: " & ensureScript, vbCritical, "WealthPipeline"
    WScript.Quit 1
End If
ps = "powershell -NoProfile -ExecutionPolicy Bypass -File """ & ensureScript & """"
rc = sh.Run(ps, 0, True)
If rc = 0 Then WScript.Quit 0

scriptPy = Chr(34) & root & "\serve_advisor.py" & Chr(34)
cmd = "cmd /c cd /d """ & root & """ && " & _
  "set WEALTH_LEADS_APP_SECRET=wealthpipeline-local-dev-only-change-me && " & _
  "set WEALTH_LEADS_ALLOW_SIGNUP=1 && " & _
  "set WEALTH_LEADS_APP_PORT=8765 && " & _
  "(where py >nul 2>&1 && py -3 " & scriptPy & ") || " & _
  "python " & scriptPy
sh.Run cmd, 0, False
