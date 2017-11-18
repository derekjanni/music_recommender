# Recommendations Engine

This repository contains code that produces content recommendations. To deploy, clone this project to `recommendations`, define your datastore in Postgres (you may need to write some custom code, depending on what your tables look like), and fire up the celery daemon with `celery -A celery_tasks.task worker -l info`. To run the script version, have a look at the `scripts` directory, which contains code to produce more human-readable results.
