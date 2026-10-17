.PHONY: test sim up-core up-batch down topics prepare replay

test:            ## 단위 테스트 (브로커 불필요)
	python -m pytest -q

sim:             ## 사전 검증 시뮬레이션 재현
	cd sim && python databrain_sim_v2.py 7 && python compare_controllers.py 7 . && python compare_sampling.py 3

up-core:         ## Kafka, Flink, ClickHouse, 제어기, 모니터링
	docker compose --profile core up -d --build

up-batch:        ## + MinIO, Spark, Airflow
	docker compose --profile core --profile batch up -d

down:
	docker compose --profile core --profile batch --profile view --profile tools down

prepare:         ## REES46 CSV → 시간순 Parquet (예: make prepare SRC=data/raw/2019-Oct.csv OUT=data/parquet/2019-10)
	python -m databrain.replayer.prepare $(SRC) $(OUT)

replay:          ## 하루치 재생 (예: make replay DAY=data/parquet/2019-10/day=2019-10-01 SPEED=100)
	docker compose run --rm replayer $(DAY) --speed $(SPEED)
