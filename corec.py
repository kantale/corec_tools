
import json

def corec_set(parameter, value):

	params_fn = "corec_parameters.json"

	with open(params_fn) as f:
		params = json.load(f)

	params[parameter] = value

	with open(params_fn, 'w') as f:
		json.dump(params, f, indent=4)

def corec_get(parameter):

	params_fn = "corec_parameters.json"

	with open(params_fn) as f:
		params = json.load(f)

	if parameter in params:
		return params[parameter]

	return None


