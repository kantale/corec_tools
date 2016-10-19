
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

def corec_lock(lock):
	locks_fn = "corec_locks.json"

	with open(locks_fn) as f:
		locks = json.load(f)

	locks[lock] = True

	with open(locks_fn, 'w') as f:
		json.dump(locks, f, indent=4)

def corec_unlock(lock):
        locks_fn = "corec_locks.json"

        with open(locks_fn) as f:
                locks = json.load(f)

        locks[lock] = False

        with open(locks_fn, 'w') as f:
                json.dump(locks, f, indent=4)



