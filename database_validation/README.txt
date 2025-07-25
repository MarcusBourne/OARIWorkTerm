The validation script is coded to automatically check for ddh_database.xlsx and IET_database.xlsx
Make sure both are in the same folder.

validated file is named ddh_database_updated.xlsx

It normalizes your Co_No column to remove all whitespace and capitalize any lowercase letters.
If Scan_Type is missing a row it will automatically fill it with "Hyperspectral VNIR-LWIR"
Any duplicate MasterNo rows will be deleted.
All data will match with its correct MasterNo row in IET_Database and change in the updated file.
