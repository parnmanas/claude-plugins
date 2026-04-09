# Snapshot file
# Unset all aliases to avoid conflicts with functions
unalias -a 2>/dev/null || true
shopt -s expand_aliases
# Check for rg availability
if ! (unalias rg 2>/dev/null; command -v rg) >/dev/null 2>&1; then
  function rg {
  local _cc_bin="${CLAUDE_CODE_EXECPATH:-}"
  [[ -x $_cc_bin ]] || _cc_bin=$(command -v claude 2>/dev/null)
  if [[ ! -x $_cc_bin ]]; then command rg "$@"; return; fi
  if [[ -n $ZSH_VERSION ]]; then
    ARGV0=rg "$_cc_bin" "$@"
  elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]] || [[ "$OSTYPE" == "win32" ]]; then
    ARGV0=rg "$_cc_bin" "$@"
  elif [[ $BASHPID != $$ ]]; then
    exec -a rg "$_cc_bin" "$@"
  else
    (exec -a rg "$_cc_bin" "$@")
  fi
}
fi
export PATH='/c/Users/parn/bin:/mingw64/bin:/usr/local/bin:/usr/bin:/bin:/mingw64/bin:/usr/bin:/c/Users/parn/bin:/c/Program Files/Python312/Scripts_:/c/Program Files/Python312_:/c/Users/parn/AppData/Local/Programs/cursor/resources/app/bin:/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v12.3/bin:/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v12.3/libnvvp:/c/Windows/system32:/c/Windows:/c/Windows/System32/Wbem:/c/Windows/System32/WindowsPowerShell/v1.0:/c/Windows/System32/OpenSSH:/c/Program Files/dotnet:/cmd:/c/Program Files/TortoiseSVN/bin:/c/Program Files (x86)/Pulse Secure/VC142.CRT/X64:/c/Program Files (x86)/Pulse Secure/VC142.CRT/X86:/c/Program Files (x86)/Common Files/Pulse Secure/TNC Client Plugin:/c/Program Files/PuTTY:/c/Program Files/Microsoft SQL Server/120/Tools/Binn:/c/Program Files (x86)/GtkSharp/2.12/bin:/c/Program Files (x86)/NVIDIA Corporation/PhysX/Common:/c/Program Files/CMake/bin:/c/Program Files/TortoiseGit/bin:/c/Program Files/NVIDIA Corporation/Nsight Compute 2023.3.1:/c/Program Files/ffmpeg/bin:/c/Program Files (x86)/Microsoft SQL Server:/c/Program Files (x86)/ePapyrus/Papyrus-PlugIn-xfa:/c/Program Files (x86)/ePapyrus/Papyrus-PlugIn-xfa/Addins:/c/WINDOWS/system32:/c/WINDOWS:/c/WINDOWS/System32/Wbem:/c/WINDOWS/System32/WindowsPowerShell/v1.0:/c/WINDOWS/System32/OpenSSH:/c/Program Files/Docker/Docker/resources/bin:/c/Program Files/Docker/Docker/frontend:/c/Program Files/cursor/resources/app/bin:/c/Program Files/nodejs:/c/Program Files/Amazon/AWSCLIV2:/c/Program Files/Warp/bin:/c/Program Files/MySQL/MySQL Shell 8.0/bin:/c/Users/parn/.local/bin:/c/Users/parn/AppData/Local/pnpm:/c/Users/parn/AppData/Local/Microsoft/WindowsApps:/c/Users/parn/AppData/Local/Programs/Microsoft VS Code/bin:/c/Users/parn/AppData/Local/Programs/Ollama:/c/Users/parn/AppData/Roaming/npm:/c/Users/parn/AppData/Local/Programs/Antigravity/bin:/c/Users/parn/.dotnet/tools:/c/Users/parn/AppData/Local/Microsoft/WinGet/Links:/c/Users/parn/.bun/bin:/c/Users/parn/.bun/bin:/usr/bin/vendor_perl:/usr/bin/core_perl:claude-config/plugins/cache/parnmanas/discord/0.0.4/bin'
