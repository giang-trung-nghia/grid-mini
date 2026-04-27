# grid-mini

step1: 
`venv\Scripts\activate`

step2: run backend
`uvicorn app.main:app --reload`

step3: run celery
`celery -A app.workers.celery_app worker --pool=solo --loglevel=info`

step4: beat celery
`celery -A app.workers.celery_app beat --loglevel=info`