# Script_bash_powershell_cmd

## Script to find duplicates files on a computer 

### the generate_hashes.ps1 or generate_hashes_multi.ps1 script file

they will generate a NDJSON file with this structure as an example:

```bash
{"Path":".\\fileName","Hash":"883820B20985ECA5F8A12FCDE7E4EB5585796DB7F6A12A0080D6D7A211AD5A0C","Size":342624256}
```

type this commande to start the scripts

if you have powershell version less then v7 then : 
```bash
.\generate_hashes.ps1 -Path "Z:\sauvegardes\series" -OutputJson "hashs_sauvegardes.json"
```
otherwise if your version is v7 or more : (multi thread script) 
```bash
.\generate_hashes_multi.ps1 -Path "Z:\sauvegardes\series" -OutputJson "hashs_sauvegardes.json"
```

### compare_hashes.ps1

This script file will take the JSON file and scan a repository given in a parameter to search for the exact same file in another folder

```bash
.\compare_hashes.ps1 -Path "Z:\sauvegardes\series" -OutputJson "hashs_sauvegardes.json"
```
