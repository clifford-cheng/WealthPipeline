' If .bat files open in an editor instead of running, double-click this.
' Opens Command Prompt and starts the advisor app from the project root.

Option Explicit

Dim sh, fso, winDir, scriptsDir, root, scriptPy, cmd

Set sh = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
winDir = fso.GetParentFolderName(WScript.ScriptFullName)
scriptsDir = fso.GetParentFolderName(winDir)
root = fso.GetParentFolderName(scriptsDir)

If Not fso.FolderExists(root & "\wealth_leads") Then
    MsgBox "Could not find wealth_leads at: " & root, vbCritical, "Equity Signal"
    WScript.Quit 1
End If

scriptPy = Chr(34) & root & "\serve_advisor.py" & Chr(34)
cmd = "cmd.exe /k cd /d """ & root & """ && " & _
  "set WEALTH_LEADS_APP_SECRET=equity-signal-local-dev-only-change-me && " & _
  "set WEALTH_LEADS_ALLOW_SIGNUP=1 && " & _
  "(py -3 " & scriptPy & " || python " & scriptPy & ")"

sh.Run cmd, 1, False
