task hello {
	String addressee
	command {
		echo "Hello ${addressee}! Welcome to Cromwell . . . on Google Cloud!"
	}
	output {
		String message = read_string(stdout())
	}
	runtime {
		docker: "ubuntu:latest"
	}
}

workflow wf_hello {
	call hello

	output {
		hello.message
	}
}