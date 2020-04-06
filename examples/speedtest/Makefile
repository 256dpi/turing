cluster:
	rm -rf ./data
	goreman start

single:
	rm -rf ./data
	go run . --id 7 --members "7@0.0.0.0:42070"

standalone:
	go run . --id 9 --standalone

in-memory:
	go run . --standalone --memory
