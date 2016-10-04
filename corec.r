


rjson_exists <- require('rjson')

if (! rjson_exists) {
	print ('rjson does not exist');
	install.packages("rjson", repos='http://cran.us.r-project.org');
}

library(rjson);

corec_get <- function(parameter) {
	json_data <- fromJSON(file="corec_parameters.json");
	return(json_data[[parameter]]);
}

corec_set <- function(parameter, value) {
	json_data <- fromJSON(file="corec_parameters.json");
	json_data[parameter] <- value;
	json_str <- toJSON(json_data);

	write(json_str, 'corec_parameters.json');
}

