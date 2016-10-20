
import os
import cgi
import sys
import json
import time
import uuid
import errno  
import logging
import subprocess

from shutil import copyfile

logging.basicConfig(level=logging.DEBUG)

defaults = {
	'parameters_filename': 'corec_parameters.json',
	'locks_filename': 'corec_locks.json',
	'exit_on_non_zero_return_code' : True,
	'pipeline_filename': 'pipeline.json',
	'progress_filename': 'corec_progress.txt',
	'mock' : False,
	'report_embed': [
		(['png', 'jpg', 'jpeg'], lambda x : '<img src="{}">'.format(x)),
		[('pdf'), lambda x: '<embed src="{}" width="500" height="375" type="application/pdf">'.format(x)],
		[('html'), lambda x: x],
	],
	'report_text': lambda x : '<p><code>{}</code></p>'.format(cgi.escape(x).encode('ascii', 'xmlcharrefreplace')), # http://stackoverflow.com/questions/1061697/whats-the-easiest-way-to-escape-html-in-python DEPRECATION NOTE
	'report_default': lambda x : '<p>{}</p>'.format(x),

	# Do not change any of these
	'parameters': {},  
	'current_progress': '',
}

class CORECException(Exception):
	pass

def now():
	return time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())


def get_uuid():
	return str(uuid.uuid4()).split('-')[-1]

def mkdir_p(path):
	'''
	http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
	'''
	try:
		os.makedirs(path)
	except OSError as exc:  # Python >2.5
		if exc.errno == errno.EEXIST and os.path.isdir(path):
			pass
		else:
			raise

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

def get_outgoing_edges(pipeline, node):
	for edge in pipeline["elements"]["edges"]:
		if get_source(edge) == get_id(node):
			yield edge

def get_ingoing_edges(pipeline, node):
	for edge in pipeline["elements"]["edges"]:
		if get_target(edge) == get_id(node):
			yield edge

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

def save_parameter(parameter, value, merge):
	load_parameters()
	if merge:
		if not parameter in defaults['parameters']:
			raise CORECException('Error in merge: Parameter {} does not exist'.format(parameter))
		
		defaults['parameters'][value] = defaults['parameters'][parameter]

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

	return "{}_{}_{}.sh".format(prefix, name.replace('|', '_'), get_uuid())

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

	if defaults['mock']:
		# We pretend to execute them
		return

	id_ = get_id(node_with_commands)
	fn = random_filename(prefix, id_)
	logging.info('Saving bash commands to {}'.format(fn))

	with open(fn, 'w') as f:
		f.write('set -e\n\n') # Stop on first error
		f.write(commands)

	command = 'bash {}'.format(fn)
	logging.info('Running: {}'.format(command))
	return_code, output, error = run_bash_command(command)
	logging.info('Return Code of command {} --> {}'.format(command, return_code))
	if return_code:
		logging.warning('RETURN CODE {} is not zero..'.format(return_code))
		if defaults['exit_on_non_zero_return_code']:
			logging.info('Exiting.. (Fail)')
			sys.exit(1)

# MANAGE LOCKS

def reset_locks():
	#Reset locks
	with open(defaults['locks_filename'], 'w') as f:
		json.dump({}, f, indent=4)

def set_lock_value(lock_name, value):
	locks_filename = defaults['locks_filename']

	if os.path.isfile(locks_filename):
		with open(locks_filename) as f:
			locks = json.load(f)
	else:
		locks = {}

	locks[lock_name] = value

	with open(locks_filename, 'w') as f:
		json.dump(locks, f, indent=4)

def set_lock(lock_name):
	set_lock_value(lock_name, True)

def unset_lock(lock_name):
	set_lock_value(lock_name, False)

def get_lock_value(lock_name):
	locks_filename = defaults['locks_filename']

	if os.path.isfile(locks_filename):
		with open(locks_filename) as f:
			locks = json.load(f)
	else:
		locks = {}

	if lock_name in locks:
		return locks[lock_name]

	return False

def get_all_locks():
	locks_filename = defaults['locks_filename']

	if os.path.isfile(locks_filename):
		with open(locks_filename) as f:
			locks = json.load(f)
	else:
		return []

	ret = [lock_name for lock_name, lock_value in locks.iteritems() if lock_value]
	return ret


# END OF MANAGE LOCKS

##################################################

# COREC REPORT

def report_html_fn():
	load_parameters()

	return os.path.join(defaults['parameters']['corec_report_dir'], 'index.html')

def report_init():

	#Create corec report directory
	corec_report_directory = "corec_report_{}".format(get_uuid())

	mkdir_p(corec_report_directory)
	logging.info('Created report directory: {}'.format(corec_report_directory))
	save_parameter('corec_report_dir', corec_report_directory, False)

	init_content = '''<!DOCTYPE html>
<html>
<body>
{content}
</body>
</html>
'''

	with open(report_html_fn(), 'w') as f:
		f.write(init_content)

def report_finalize():

	html_filename = report_html_fn()
	with open(html_filename) as f:
		html_filename_content = f.read()

	html_filename_new_content = html_filename_content.format(content='')

	with open(html_filename, 'w') as f:
		f.write(html_filename_new_content)

	logging.info('HTML Report is available at: {}'.format(html_filename))

def report_add(content):
	'''
	content is always string
	'''

	load_parameters()
	corec_report_directory = defaults['parameters']['corec_report_dir']

	assert type(content).__name__ in ['unicode', 'str']

	html_to_add = ''

	if os.path.isfile(content):
		'''
		There is a file with this name
		Get the extension
		'''
		dest = os.path.join(corec_report_directory, content)

		#Copy to report directory
		copyfile(content, dest)

		extension = os.path.splitext(content)[1].lower()
		filename = os.path.split(content)[1]

		for formats, html_function in defaults['report_embed']:
			if extension in formats:
				html_to_add = html_function()

		if not html_to_add:
			#Trying to embed the whole file as txt file
			with open(content) as f:
				text = f.read()

			html_to_add = '<p>File: <a href="{}">{}</a>:</p>'.format(content, content)
			html_to_add = defaults['report_text'](text)

		logging.info('Added in report file: {}'.format(content))

	else:
		html_to_add = defaults['report_default'](content)
		logging.info('Added in report string: {}'.format(content))

	html_filename = report_html_fn()
	with open(html_filename) as f:
		html_filename_content = f.read()

	html_filename_new_content = html_filename_content.format(content=html_to_add + '\n{content}')

	with open(html_filename, 'w') as f:
		f.write(html_filename_new_content)


# END OF COREC REPORT 

##################################################

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

	#Perhaps this step has input parameters that are satisfied by other steps
	for outgoing_edge in get_outgoing_edges(pipeline, node):
		if get_kind(outgoing_edge) == 'Needs_Parameter':
			parameter_node_id = get_target(outgoing_edge)
			parameter_node = get_node(pipeline, parameter_node_id)
			# Check if this parameter is set by any other tool
			for ingoing_edge in get_ingoing_edges(pipeline, parameter_node):
				if get_kind(ingoing_edge) == 'Sets_Outputs':
					dependent_step_id = get_source(ingoing_edge)
					if dependent_step_id != id_:
						# This is a dependent step
						dependent_step = get_node(pipeline, dependent_step_id)
						execute_step(pipeline, node=dependent_step)

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
				# this edge is a Step and has an unsatisfied output
				#We have to execute this step
				execute_step(pipeline, node=step_node)


def satisfy_outputs(pipeline, output_nodes):

	total = len(output_nodes)
	logging.info('Total unsatisfied output nodes: {}'.format(total))
	for output_node_index, output_node in enumerate(output_nodes):
		load_parameters()
		if not get_id(output_node) in defaults['parameters']:
			
			while True: # Repeatidly try to satisfy this step until there is not lock
				logging.info('Satisfying output node {}/{}: {}'.format(output_node_index+1, total, get_id(output_node)))
				satisfy_output(pipeline, output_node)
				locks = get_all_locks()
				if locks:
					logging.info('Found these locks: {}. Trying to satisfy the output again'.format(str(locks)))
				else:
					break
		else:
			logging.info('Output node {}/{}: {} has already been satisfied'.format(output_node_index+1, total, get_id(output_node)))


def show_results(output_nodes):

	load_parameters()
	logging.info("RESULTS")
	logging.info("=======")

	for output_node in output_nodes:
		output_node_id = get_id(output_node)
		if defaults['mock']:
			logging.info('     {} = {}'.format(output_node_id, '<MOCKING MODE>'))
		else:
			if output_node_id in defaults['parameters']:
				logging.info('     {} = {}'.format(output_node_id, defaults['parameters'][output_node_id]))
			else:
				logging.info('     {} = {}'.format(output_node_id, '<NOT SET>'))
	logging.info('FINISH')

def execute_cy_pipeline(pipeline):
	
	elements = pipeline['elements']
	nodes = elements['nodes']

	notset_parameters = get_notset_parameters(pipeline)
	input_parameters(notset_parameters)
	output_nodes = get_notset_outputs(pipeline)
	satisfy_outputs(pipeline, output_nodes)

	show_results(output_nodes)


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
	if 'mock' in kwargs:
		defaults['mock'] = kwargs['mock']
		if defaults['mock']:
			logging.info('Running in "mocking" mode (nothing will actually happen)')

	if 'ignore_return_code' in kwargs:
		if kwargs['ignore_return_code']:
			logging.info('Ignoring non-positive return codes')
			defaults['exit_on_non_zero_return_code'] = False

	reset_locks()
	delete_progress()
	report_init()
	execute_pipeline(kwargs['pipeline'])
	report_finalize()

@command_line
def corec_set(parameter, value, merge, **kwargs):
	save_parameter(parameter, value, merge)

@command_line
def corec_get(parameter, **kwargs):
	load_parameters()
	if parameter in defaults['parameters']:
		print defaults['parameters'][parameter]
	else:
		print 'COREC_UNSET'

@command_line
def corec_lock(lock_name, **kwargs):
	set_lock(lock_name)

@command_line
def corec_unlock(lock_name, **kwargs):
	unset_lock(lock_name)

@command_line
def corec_report(content, **kwargs):
	report_add(content)


# ========================== COREC COMMANDS ==================



