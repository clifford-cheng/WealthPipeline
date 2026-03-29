' Double-click this if .bat files open in Cursor/VS Code instead of running.
' Opens Command Prompt here and starts the WealthPipeline dashboard.

Dim sh, fso, folder, cmd

Set sh = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
folder = fso.GetParentFolderName(WScript.ScriptFullName)

If Not fso.FolderExists(folder & "\wealth_leads") Then
    MsgBox "Could not find the wealth_leads folder next to this script.", vbCritical, "WealthPipeline"
    WScript.Quit 1
End If

' Try py -3 first (Windows), then python
cmd = "cmd.exe /k cd /d """ & folder & """ && (py -3 -m wealth_leads serve || python -m wealth_leads serve)"

sh.Run cmd, 1, False
