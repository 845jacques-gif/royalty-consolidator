@echo off
echo ============================================
echo   Royalty Consolidator - Demo Mode
echo ============================================
echo.

cd /d "%~dp0"

echo Restoring demo enrichment cache...
python -c "import json; tracks={'USRC12500001':('Golden Hour','Maya Santos','2024-03-15'),'USRC12500002':('Midnight Drive','The Velvet Keys','2024-06-01'),'USRC12500003':('Neon Lights','DJ Prism','2024-09-10'),'USRC12500004':('Wildflower','Luna Park','2024-01-20'),'USRC12500005':('City Rain','Marcus Cole','2024-11-05'),'USRC12500006':('Ocean Waves','The Drifters','2023-07-22'),'USRC12500007':('Starlight','Aria Moon','2024-04-30'),'USRC12500008':('Thunder Road','Black Canyon','2023-12-01'),'USRC12500009':('Paper Planes','Indie Folk Co','2024-08-15'),'USRC12500010':('Electric Dreams','Synthwave Collective','2025-01-01')}; cache={k:{'release_date':v[2],'source':'MB','track_name':v[0],'artist_name':v[1],'looked_up':True} for k,v in tracks.items()}; json.dump(cache,open('release_date_cache.json','w'),indent=2); print(f'  Cache ready: {len(cache)} tracks')"

echo Starting server...
echo.
echo   Open in browser:  http://localhost:5000/upload?demo=1
echo.
echo   Press Ctrl+C to stop the server.
echo ============================================
echo.

python app.py
