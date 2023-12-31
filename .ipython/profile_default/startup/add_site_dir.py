import os
import site

NB_USER = os.getenv("NB_USER")

if NB_USER:
    site.addsitedir(f"/home/{NB_USER}")
