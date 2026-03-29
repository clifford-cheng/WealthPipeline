' Starts WealthPipeline dashboard server with no window (runs via cmd).
' Use "Add WealthPipeline to Windows Startup.bat" to run this automatically at login.

Option Explicit

Dim sh, fso, folder, cmd

Set fso = CreateObject("Scripting.FileSystemObject")
folder = fso.GetParentFolderName(WScript.ScriptFullName)

If Not fso.FolderExists(folder & "\wealth_leads") Then
    MsgBox "Could not find wealth_leads folder next to this script.", vbCritical, "WealthPipeline"
    WScript.Quit 1
End If

Set sh = CreateObject("WScript.Shell")

' Avoid stacking many servers on the same port (Windows can get into a bad state).
Dim ps, rc
ps = "powershell -NoProfile -ExecutionPolicy Bypass -Command ""if (Get-NetTCPConnection -LocalPort 8765 -State Listen -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }"""
rc = sh.Run(ps, 0, True)
If rc = 0 Then WScript.Quit 0

cmd = "cmd /c cd /d """ & folder & """ && (where py >nul 2>&1 && py -3 -m wealth_leads serve --no-browser) || python -m wealth_leads serve --no-browser"
' 0 = hidden window
sh.Run cmd, 0, False
