module hop

go 1.24.3

require (
	github.com/google/uuid v1.6.0
	github.com/xinix00/hoplock v0.0.0-00010101000000-000000000000
	github.com/xinix00/hoplockserver v0.0.0-00010101000000-000000000000
	gopkg.in/yaml.v3 v3.0.1
)

replace (
	github.com/xinix00/hoplock => ../../../haaslock
	github.com/xinix00/hoplockserver => ../hoplockserver
)
