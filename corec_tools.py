
import os
import sys
import json
import uuid
import logging
import subprocess

logging.basicConfig(level=logging.DEBUG)

defaults = {
	'parameters_filename': 'corec_parameters.json',
	'exit_on_non_zero_return_code' : True,
	'pipeline_filename': 'pipeline.json',
	'progress_filename': 'corec_progress.txt',

	# Do not change any of these
	'parameters': {},  
	'current_progress': '',
}

def is_parameter(node):
	return node['data']['kind'] == 'Parameter'

def is_output(node):
	return node['data']['kind'] == 'Output'

def get_id(element):
	return element['data']['id']

def get_target(edge):
	return edge["data"]["target"]

def get_source(edge):
	return edge["data"]["source"]

def get_kind(element):
	return element["data"]["kind"]

def get_node(pipeline, id_):
	for node in pipeline["elements"]["nodes"]:
		node_id = get_id(node)
		if node_id == id_:
			return node
		else:
			node_id_s = node_id.split('|')
			id_s = id_.split('|')
			if len(node_id_s) == 3 and len(id_s) == 2:
				if id_s[0] == node_id_s[0] and id_s[1] == node_id_s[2]:
					return node

def load_pipeline():

	if not os.path.isfile(defaults["pipeline_filename"]):
		return False

	with open(defaults["pipeline_filename"]) as f:
		pipeline = json.load(f)

	return pipeline


def load_parameters():
	parameters_filename = defaults['parameters_filename']

	if not os.path.isfile(parameters_filename):
		with open(parameters_filename, 'w') as f:
			json.dump({}, f)

	with open(parameters_filename) as f:
		parameters = json.load(f)

	defaults['parameters'] = parameters

def save_parameter(parameter, value):
	load_parameters()
	if merge:
		if not value in defaults['parameters']:
			logging.warning('Value: {} is NOT in the parameters. Cannot merge.'.format(value))
		else:
			defaults['parameters'][parameter] = defaults['parameters'][value]

	else:
		defaults['parameters'][parameter] = value
	save_parameters_file()


def set_up_environment():
	# dir_path = os.path.dirname(os.path.realpath(__file__)) 
	cwd = os.getcwd()
	if not cwd in os.environ["PATH"]:
		os.environ["PATH"] = os.environ["PATH"] + ':' + cwd

def command_line(f):
	def wrapper(*args, **kwargs):
		set_up_environment()
		load_parameters()
		kwargs['pipeline'] = load_pipeline()

		ret = f(*args, **kwargs)

		return ret

	return wrapper


def parameter_gets_set(pipeline, parameter):
	'''
	Check if a parameter is set by a step
	'''

	for edge in pipeline["elements"]["edges"]:
		
		if get_target(edge) == get_id(parameter) and get_kind(edge) == "Sets_Outputs":
			return True

	return False

def output_gets_set(pipeline, output):
	'''
	Check if an outputs is required by any other step as a parameter
	so that this is an intermediate output.
	'''
	for edge in pipeline["elements"]["edges"]:
		if get_target(edge) == get_id(output) and get_kind(edge) == "Needs_Parameter":
			return True

	return False

def get_notset_parameters(pipeline):

	ret = []
	for node in pipeline["elements"]["nodes"]:
		if is_parameter(node):
			#This is a parameter.
			#Is this the output of any step?
			if not parameter_gets_set(pipeline, node):
				ret.append(node)

	return ret

def get_notset_outputs(pipeline):

	ret = []
	for node in pipeline["elements"]["nodes"]:
		if is_output(node):
			#This an output
			# Maybe it is required by another step.
			if not output_gets_set(pipeline, node):
				ret.append(node)

	return ret

def save_parameters_file():
	with open(defaults['parameters_filename'], 'w') as f:
		f.write(json.dumps(defaults['parameters'], indent=4) + '\n')


def input_parameters(parameters):

	for parameter in parameters:
		p_id = get_id(parameter)
		logging.info('Found unsatisfied input parameter: {}'.format(p_id))

		if p_id in defaults['parameters']:
			p_value = defaults['parameters'][p_id]
			logging.info('Using saved value for parameter {}={}'.format(p_id, p_value))
		else:
			request_str = 'Insert the value of parameter: {} : '.format(p_id)
			p_value = raw_input(request_str)
			defaults['parameters'][p_id] = p_value

	save_parameters_file()


def random_filename(prefix, name):

	return "{}_{}_{}.sh".format(prefix, name.replace('|', '_'), str(uuid.uuid4()).split('-')[-1])

def run_bash_command(command):

	# Get progress
	progress = read_progress()

	process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)

	for line in iter(process.stdout.readline, ""):
		logging.info('{} --> {} -> {}'.format(progress, command, line.replace('\n', '')))

	output, error = process.communicate()
	return_code = process.returncode

	return return_code, output, error

def execute_commands(prefix, node_with_commands, commands):
	id_ = get_id(node_with_commands)
	fn = random_filename(prefix, id_)
	logging.info('Saving bash commands to {}'.format(fn))

	with open(fn, 'w') as f:
		f.write('set -e\n\n') # Stop on first error
		f.write(commands)

	command = 'bash {}'.format(fn)
	logging.info('Running: {}'.format(command))
	return_code, output, error = run_bash_command(command)
	logging.info('Return Code: {}'.format(return_code))
	if return_code:
		logging.warning('RETURN CODE {} is not zero..'.format(return_code))
		if defaults['exit_on_non_zero_return_code']:
			logging.info('Exiting.. (Fail)')
			sys.exit(1)

def read_progress():
	progress_filename = defaults['progress_filename']
	if not os.path.isfile(progress_filename):
		with open(progress_filename, 'w') as f:
			pass # Just create it

	with open(progress_filename) as f:
		p = f.read()

	return p

def append_progress(progress):
	progress_filename = defaults['progress_filename']

	with open(progress_filename, 'a') as f:
		f.write(progress)	

def save_progress(progress):
	progress_filename = defaults['progress_filename']

	with open(progress_filename, 'w') as f:
		f.write(progress)

def delete_progress():
	progress_filename = defaults['progress_filename']
	if os.path.isfile(progress_filename):
		os.remove(progress_filename)

def has_progress(progress_string):
	def decorator(f):
		def wrapper(*args, **kwargs):

			local_progress_string = " --> {}{}".format(progress_string, get_id(kwargs['node']))
			current_progress = read_progress()
			append_progress(local_progress_string)
			ret = f(*args, **kwargs)
			save_progress(current_progress)
			return ret
		return wrapper
	return decorator

@has_progress('STEP : ')
def execute_step(pipeline, **kwargs):
	node = kwargs['node']
	id_ = get_id(node)
	logging.info('Executing step: {}'.format(id_))

	commands = node["data"]["bash_commands"]
	execute_commands('step', node, commands)

@has_progress('INSTALL : ')
def install_tool(pipeline, **kwargs):
	node = kwargs['node']
	id_ = get_id(node)

	logging.info('Installing tool: {}'.format(id_))

	installation = node["data"]["installation"]
	execute_commands('tool', node, installation)

def satisfy_output(pipeline, output_node):
	# Get all steps that have this output_node

	for edge in pipeline["elements"]["edges"]:
		if get_kind(edge) != 'Sets_Outputs':
			continue

		edge_target = get_target(edge)
		if edge_target == get_id(output_node):
			step_node = get_node(pipeline, get_source(edge))
			if get_kind(step_node) == 'Step':
				#We have to execute this step
				execute_step(pipeline, node=step_node)


def satisfy_outputs(pipeline, output_nodes):

	total = len(output_nodes)
	logging.info('Total unsatisfied output nodes: {}'.format(total))
	for output_node_index, output_node in enumerate(output_nodes):
		logging.info('Satisfying output node {}/{}: {}'.format(output_node_index+1, total, get_id(output_node)))
		satisfy_output(pipeline, output_node)

def execute_cy_pipeline(pipeline):
	
	elements = pipeline['elements']
	nodes = elements['nodes']

	notset_parameters = get_notset_parameters(pipeline)
	input_parameters(notset_parameters)
	output_nodes = get_notset_outputs(pipeline)
	satisfy_outputs(pipeline, output_nodes)


def execute_pipeline(pipeline):

	execute_cy_pipeline(pipeline)

# ========================== COREC COMMANDS ==================

@command_line 
def corec_requires(step, **kwargs):
	logging.info('Satisfying requirement: {}'.format(step))
	
	pipeline = kwargs['pipeline']
	tool_node = get_node(pipeline, step)

	install_tool(pipeline, node=tool_node)

@command_line
def corec_init(**kwargs):
	#Delete progress file
	delete_progress()
	execute_pipeline(kwargs['pipeline'])

@command_line
def corec_set(parameter, value, merge, **kwargs):
	save_parameter(parameter, value, merge)

# ========================== COREC COMMANDS ==================

	

