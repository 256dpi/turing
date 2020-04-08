cluster:
	rm -rf ./data
	goreman start # ids 1-3

single:
	rm -rf ./data
	go run . --id 4 --members "4@0.0.0.0:42070"

standalone:
	rm -rf ./data
	go run . --id 0 --standalone

in-memory:
	go run . --id 0 --standalone --memory
