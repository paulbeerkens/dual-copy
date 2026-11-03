@echo off
setlocal
cd /d "%~dp0"
rem Check the version and GUI support before starting the app.
py -3 -c "import sys, tkinter; assert sys.version_info >= (3,10); r=tkinter.Tk(); r.withdraw(); r.destroy()" >nul 2>&1
if not errorlevel 1 (
  py -3 dual_copy.py
  goto finished
)
python -c "import sys, tkinter; assert sys.version_info >= (3,10); r=tkinter.Tk(); r.withdraw(); r.destroy()" >nul 2>&1
if not errorlevel 1 (
  python dual_copy.py
  goto finished
)
for /d %%D in ("%LocalAppData%\Programs\Python\Python*" "%ProgramFiles%\Python*" "%LocalAppData%\Python\pythoncore-*") do (
  if exist "%%~D\python.exe" (
    "%%~D\python.exe" -c "import sys, tkinter; assert sys.version_info >= (3,10); r=tkinter.Tk(); r.withdraw(); r.destroy()" >nul 2>&1
    if not errorlevel 1 (
      "%%~D\python.exe" dual_copy.py
      goto finished
    )
  )
)
echo.
echo A working Python 3.10+ installation with Tkinter was not found.
echo Install Python for Windows from https://www.python.org/downloads/windows/
echo Include Tcl/Tk and select Add Python to PATH during installation.
echo Then double-click this launcher again.
pause
exit /b 1
:finished
if errorlevel 1 pause
