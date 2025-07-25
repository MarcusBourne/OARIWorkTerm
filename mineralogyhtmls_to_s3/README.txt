this script will automatically upload _html.html documents into every drillcore folder in the s3.

this script scans your selected drive for all folders ending in _Mineralogy.
It will save upload them to the matching s3 bucket destination.
uncomment lines 55 to 65 to save files locally instead.
creds.txt is you AWS info in the format "accesskey, secret key"
