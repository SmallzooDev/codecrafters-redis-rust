pub fn parse_env(args: Vec<String>) -> Result<Vec<(String, String)>, String> {
    if args.len() <= 1 {
        return Err("No configuration arguments provided to parse".into());
    }

    let mut result = Vec::new();
    let mut arg_index = 1;

    while arg_index < args.len() {
        match args[arg_index].as_str() {
            "--dir" => {
                if arg_index + 1 < args.len() {
                    result.push(("dir".into(), args[arg_index + 1].clone()));
                    arg_index += 2;
                } else {
                    return Err("Argument Error: --dir option requires an argument".into());
                }
            }
            "--dbfilename" => {
                if arg_index + 1 < args.len() {
                    result.push(("file_name".into(), args[arg_index + 1].clone()));
                    arg_index += 2;
                } else {
                    return Err("Argument Error: --dbfilename option requires an argument".into());
                }
            }
            "--port" => {
                if arg_index + 1 < args.len() {
                    result.push(("port".into(), args[arg_index + 1].clone()));
                    arg_index += 2;
                } else {
                    return Err("Argument Error: --port option requires an argument".into());
                }
            }

            _ => return Err(format!("Argument Error: '{}' is an unknown option", args[arg_index])),
        }
    }

    Ok(result)
}