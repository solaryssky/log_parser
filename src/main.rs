///code example
///```obss_log all 1800 all```
///version info
///```obss_log -version```


use jwalk::{WalkDir, Parallelism};
use std::fs;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use std::path::{PathBuf, Path};
use chrono::{offset::TimeZone, NaiveDateTime, DateTime, Local};
use regex::Regex;
use xml::common::Position;
use xml::reader::{ParserConfig, XmlEvent};
//use rand::Rng;
//use postgres::{Client, NoTls};

extern crate rev_buf_reader;
use rev_buf_reader::RevBufReader;



/// This function return `Vec<String>` from directory listing
    fn log_parser()-> Vec<String> {

  
    let _guard = sentry::init(("https://url.ru", 
    sentry::ClientOptions {
            release: sentry::release_name!(),
            traces_sample_rate: 0.2, //send 20% of transaction to sentry
            ..Default::default()
        }));
    
    let name = hostname::get().unwrap();
    let hostname = name.to_string_lossy();

        sentry::configure_scope(|scope| {
            scope.set_user(Some(sentry::User {
                id: Some(111.to_string()),
                email: Some("dima@yandex.ru".to_owned()),
                username: Some("dima".to_owned()),                
                ..Default::default()
            }));
           scope.set_tag("ftpUploader log", &hostname);
        });

        let tx_ctx = sentry::TransactionContext::new(
            &hostname,
            "main transaction",
        );
        let transaction = sentry::start_transaction(tx_ctx);
        

        sentry::capture_message("Im start!", sentry::Level::Info);
   

    //argv key for interval log records
    let interval:i64  = match std::env::args().nth(4).as_deref() {
    Some(val) => val.parse::<i64>().unwrap_or(3600),
    None => 600, //default 10 min
    };

    let log_interval:u64 = 600; //if log not update > 600 sec
    let file_log = PathBuf::from(r"/app/ftpUpload/logs/log");
    let pitstop:bool = Path::new("/app/pitstop").try_exists().expect("Can't check pitstop file");
    
    //get value from external file for path-time settings
    let file_data = fs::read_to_string("/app/config_time.csv").expect("The external file time could not be read");
    let mut hash_map_time:HashMap<&str, u64> = HashMap::new();
    for line in file_data.lines() {
        let index_sub = line.find(";").unwrap();
        let path_config_time = &line[..index_sub];
        let second_config_time_str = &line[index_sub + 1..];
        let second_config_time_int:u64 = second_config_time_str.parse().expect("Not a valid number");
        hash_map_time.insert(path_config_time, second_config_time_int);
    }
    
    let mut sli:f32 = rand::random_range(99.95..100.00);
    let time_file_log = file_log.metadata().unwrap().modified();
    let _last_modified_file_log = time_file_log.expect("REASON").elapsed().unwrap();
    let _sec_mttime_file_log = Duration::as_secs(&_last_modified_file_log);
    let today = Local::now();
    let mut print_data: String;
    let mut output_string:Vec<String> = Vec::new();
    //let _ = fs::create_dir("/dev/null/");
    
    
    let span_xml = transaction.start_child("read xml", "read config from ftpUploader.xml");
    //XML config parse 
    let workconfig = File::open("/app/ftpUpload/lastCfg/workConfig").unwrap();
    let mut reader_workconfig = ParserConfig::default().ignore_root_level_whitespace(false).create_reader(BufReader::new(workconfig));
    let mut hash_map_type:HashMap<String, String> = HashMap::new();
    let mut hash_map_server:HashMap<String, String> = HashMap::new();  
    let mut hash_map_srcdir:HashMap<String, String> = HashMap::new();  
    let mut hash_map_dstdir:HashMap<String, String> = HashMap::new(); 
    let mut scores:HashMap<String, i32> = HashMap::new();
    //let type_agent1 = "FtpUpload";
   // let type_agent2 = "FtpDownload";
    let binding = "not_found".to_string();

    loop {
        match reader_workconfig.next() {
            Ok(e) => {

                match e {

                    XmlEvent::EndDocument => {
                        break;
                    }

                      XmlEvent::StartElement { name, attributes, .. } => {
                        

                     //   if name.local_name.contains(type_agent1) || name.local_name.contains(type_agent2) {   
                        
                               let mut names = String::new();                                
                               let mut ftpserver = String::new(); 
                               let mut srcdir = String::new(); 
                               let mut dstdir = String::new(); 
                            
                            

                            for attr in attributes {

                                match attr.name.local_name.as_str() {
                                      "ftpServer" => ftpserver += &attr.value,
                                           "name" => names += &attr.value,
                                           "srcDir" => srcdir += &attr.value,
                                           "dstDir" => dstdir = attr.value,
                                                _ => (),
        
                                  }
                                 

                            }

                            if ftpserver.is_empty() {
                                ftpserver = "localhost".to_string();
                            }

                            if dstdir.is_empty() {
                                dstdir = "/dev/null".to_string();
                            }

                            if srcdir.is_empty() {
                                srcdir = "/dev/null".to_string();
                            }


                            //add to hashmap
                        if names != ""  {
                            hash_map_type.insert(names.to_string(), name.local_name.to_string());
                            hash_map_server.insert(names.to_string(), ftpserver.to_string());
                            
                        if !srcdir.contains("/dev/null") {
                            hash_map_srcdir.insert(names.to_string(), srcdir.to_string());                            
                        //если нужно передавать данные по неактивным агентам srcdir
                          scores.insert("hostname=".to_owned() + &ftpserver + ",name=" + &names + " " + &srcdir + "/", 0);
                        }  
                        
                        if !dstdir.contains("/dev/null") {
                            hash_map_dstdir.insert(names.to_string(), dstdir.to_string());
                        //если нужно передавать данные по неактивным агентам dstdir
                          scores.insert("hostname=".to_owned() + &ftpserver + ",name=" + &names + " " + &dstdir + "/", 0);
                        }
                        
                        }
                    
                            


                    //}
                        
                    },

                    _ => ()
                }
            }
            Err(e) => {
                eprintln!("Error at {}: {e}", reader_workconfig.position());
                break;
            },
        }
    }


    span_xml.finish();

    //argv key for type files
    let key1:i8  = match std::env::args().nth(1).as_deref() {
        Some("all") => 1,
        Some("bad") => 2,
        Some("version") => 3,
        None => 1,
           _ => 1,
    };

    //argv key in second for older files
    let mut oldfiles:u64  = match std::env::args().nth(2).as_deref() {
                Some(val) => val.parse::<u64>().unwrap_or(1800),
                None => 2000,
        
            };     

    
    //argv key for type directory
    let in_out_key:i8  = match std::env::args().nth(3).as_deref() {
        Some("all") => 1,
        Some("in") => 2,
        Some("out") => 3,
        Some("ses") => 4,
        None => 2,
           _ => 2,
    };

    
if key1 == 3 {
    let version = env!("CARGO_PKG_VERSION");
    let author = env!("CARGO_PKG_AUTHORS");
        print_data = "Program Version: ".to_owned() + version + " Author: " + author;
        output_string.push(String::from(&print_data));
        return output_string;
}
    


if _sec_mttime_file_log > log_interval{
        print_data = "q".to_owned() +&hostname+ ",name=ftpUpload,dir=/app/ftpUpload/logs/log ok=0,bad=" +&_sec_mttime_file_log.to_string()+",log=0";
        output_string.push(String::from(&print_data));
        return output_string;
    }
    else{
        print_data = "q".to_owned() +&hostname+ ",name=ftpUpload,dir=/app/ftpUpload/logs/log ok=0,bad=0,log=1";
        output_string.push(String::from(&print_data));
    }





    //let now = Local::now().naive_local();
    //let mut previous_hour:u8 = now.hour().try_into().unwrap();
    
    //if previous_hour != 0{
   //    previous_hour = previous_hour - 1;
    //}
    
    
    let mut scores_end:HashMap<String, i64> = HashMap::new();
    let mut scores_size:HashMap<String, i64> = HashMap::new();
    let max_depth: usize = 1;
    let parent_in = String::from("/data/in/");
    let parent_out = String::from("/data/out/");
        scores.insert("hostname=localhost,name=parent ".to_owned() + &parent_in, 0);
        scores.insert("hostname=localhost,name=parent ".to_owned() + &parent_out, 0);

    
    let span_log = transaction.start_child("read log", "read log file to buffer revers");
    //read log file 
    let input = File::open(file_log).expect("Should have been able to read the file");
    //let f = BufReader::new(input);         
    let f = RevBufReader::new(input); 
    //let mut re = Regex::new(r"([^,]*)([^\[]*\[)(.*)(\][^']+')([^']*\/)(.*'\s(t|to)\s\')([^']*\/)(.*)$").unwrap();
    
    // in_out_key == 1
    let mut re = Regex::new(r"^([^,]*)([^\[]*\[)(.*)(\][^']+')([^']*/)(.*'\s(t|to)\s')([^']*/)([^']*)(.*(size:|sz)\s)(\d+)$").unwrap();
    
    if in_out_key == 2 {
           re = Regex::new(r"([^,]*)([^\[]*\[)(.*)(\][^']+')([^']*CDR\/IN[^']*\/)(.*')(.*)").unwrap();
    }
    else if in_out_key == 3 {
           re = Regex::new(r"([^,]*)([^\[]*\[)(.*)(\][^']+')([^']*CDR\/OUT[^']*\/)(.*')(.*)").unwrap();
    }  
    else if in_out_key == 4 {
           re = Regex::new(r"([^,|.]*)([^\[]*\[)(.*)(\]\sXmm.*connect\sto\s)(.*)(\s:\ssession\sis\sdown)(.*filesInQueue\=)(\d+)").unwrap();
    }
    
   let re2 = Regex::new(r"blackList|Stop|Begin|main|WatchDog").unwrap();
   let re3 = Regex::new(r"ERROR").unwrap();
   
   //read line from file
    for line in f.lines(){  

        let mut line_r = line.expect("Unable to read line");
        let date_time_str = &line_r[0..19];
        let date_time_log = NaiveDateTime::parse_from_str(date_time_str, "%Y-%m-%d %H:%M:%S").expect("error parse date on log");
        let date_time_log_sec: DateTime<Local> = Local.from_local_datetime(&date_time_log).unwrap();
        let diff = today - date_time_log_sec;
        let diff = diff.num_seconds();
    
    //if datetime in log > interval => break for loop
    if diff > interval{    
        break;
    }

    
    if re2.is_match(&line_r){
            continue;
        }

        
    if re3.is_match(&line_r){       
            
            let l_index = line_r.find("]").expect("No find index in string");  
            let f_index = line_r.find("[").expect("No find index in string");
            let a_name_slice = &line_r[f_index + 1..l_index];

            let error_agent_ftpserver = hash_map_server.get(a_name_slice).unwrap_or(&binding);
            let error_agent_srcdir = hash_map_srcdir.get(a_name_slice).unwrap_or(&binding);     
            let error_agent_dstdir = hash_map_dstdir.get(a_name_slice).unwrap_or(&binding); 
            let l_slice = line_r[0..l_index + 1].to_owned() + " done ip "+ error_agent_ftpserver +" f '"+ error_agent_srcdir +"/' t '"+ error_agent_dstdir +"/' sz 0";               
                line_r = String::from(l_slice); 
        }    
      



//match for statistic and sli
    match re.captures(&line_r) { 
       Some(caps) => { 
     

       // let line_hour: u8 = line_r[11..13].parse().unwrap();
        //let line_minute: u8 = line_r[14..16].parse().unwrap();
        
        
//if hours from log < current hours => continue loop
      //  if line_hour < previous_hour {
       //     continue;
      //  }

        //let _date_time = caps.get(1).unwrap().as_str();
        let mut type_message = caps.get(2).unwrap().as_str();
                type_message = &type_message[5..10];
        let _agent_name = caps.get(3).unwrap().as_str();
        let _host_name_tmp = caps.get(4).unwrap().as_str();
        let _dir_name = caps.get(5).unwrap().as_str();
        let  dir_name_to = caps.get(8).unwrap().as_str();
        //let  file_name = caps.get(9).unwrap().as_str();
        let file_size_str = caps.get(12).unwrap().as_str();
        let file_size_int: i64 = file_size_str.parse().unwrap();

        scores_size.entry(_agent_name.to_string()).and_modify(|count| *count += file_size_int).or_insert(file_size_int);


        
        
        let mut _host_name = "localhost";
    if _host_name_tmp.contains("] done ip"){
            let _hst_substr_1 = &_host_name_tmp[10..];
            let len = _hst_substr_1.len() - 4;
                _host_name = &_hst_substr_1[..len];

                
        }
        
            let type_agent = hash_map_type.get(_agent_name).unwrap_or(&binding);          
            let mut _agent_dir = "hostname=".to_owned() + &_host_name + ",name=" +_agent_name + " " + &_dir_name;


            let mut _agent_dir_to = String::new();

       if type_agent.contains("Copy"){
                   _agent_dir_to = "hostname=".to_owned() + &_host_name + ",name=" +_agent_name + " " + &dir_name_to;
       }

        if type_message == "ERROR"{            


                    _agent_dir = "hostname=".to_owned() + &_host_name + ",type=err,name=" +_agent_name + " " + &_dir_name;

              }
      	



      //  if _agent_name.to_string() == _args_agent.to_string(){
            
            //let _clone_line_r = &line_r.clone();
        

           // let _dt = NaiveDateTime::parse_from_str(_date_time, "%Y-%m-%d %H:%M:%S,%3f").unwrap();
            //  let _dt = NaiveDateTime::parse_from_str(_date_time, "%Y-%m-%d %H:%M:%S").unwrap();

           // let diff = now.signed_duration_since(_dt);
            //let dur = diff.num_seconds();

           // if dur < interval{
                        
              
                //add to hashmap
                scores.entry(_agent_dir).and_modify(|count| *count += 1).or_insert(1);
                scores.entry(_agent_dir_to).and_modify(|count| *count += 1).or_insert(1);
                

               // scores.insert(String::from(_agent_dir), i);
               // scores.insert(_agent_dir, i);
                

               
            //}



                
                

           
    
    
           // }
    }
    None =>{
    }
}


       

       }

    span_log.finish();

// read agent hash map  
if scores.is_empty(){
           print_data = "q".to_owned() +&hostname+ ",name=hashmap,dir=/app/ftpUpload/logs/log ok=0,bad=0,size=0,log=0,sli=100";
           output_string.push(String::from(&print_data));
}


else{   

    for (key, value) in &scores {

         let re_hash = Regex::new(r"(.*)\s(.*)$").unwrap();
         match re_hash.captures(&key) {
            Some(caps_hash) => {
                let _agent_hash = caps_hash.get(1).unwrap().as_str();
                let path_hash = caps_hash.get(2).unwrap().as_str();
                let _str_hash_key = _agent_hash.to_owned() + " " + &path_hash + " " + &value.to_string();                                 
                     scores_end.insert(_str_hash_key.clone(), 0);
                     
                              
                
                //check files in dir
                
               /*
               //recursive directory off
               let dirs = fs::read_dir(path_hash).unwrap();
               let entries: Vec<PathBuf> = dirs.filter(Result::is_ok).map(|e| e.unwrap().path()).collect();
                for file in entries {
                    if file.as_path().exists() && file.metadata().unwrap().is_file() {
                */                        

               
      //recursive directory on
      let span_dir = transaction.start_child("check directory", "check directory on older files");



if !path_hash.contains("/dev/null") {

  for file in WalkDir::new(path_hash).parallelism(Parallelism::RayonNewPool(10)).max_depth(max_depth).into_iter().filter_map(|file| file.ok()) {
        
        if file.path().exists() && file.metadata().expect("no file found for get metadata").is_file() {
            let time = file.metadata().expect("error get metadata").accessed();
            let _last_modified = time.expect("duration atime error").elapsed().unwrap();
            let one_sec = Duration::as_secs(&_last_modified);
 
    
      if hash_map_time.contains_key(path_hash){
             oldfiles = hash_map_time.get(&path_hash).map(|&x| x as u64).unwrap_or(2500);
        }

       if one_sec > oldfiles  {   
            scores_end.entry(_str_hash_key.clone()).and_modify(|count| *count += 1).or_insert(1);
        }

          
        else if !scores_end.contains_key(&_str_hash_key){
            scores_end.insert(_str_hash_key.clone(), 0);
            //println!("{} {:?} {}  | {}", file.path().display(), &_last_modified, one_sec, _str_hash_key);
        }


      }
        
   }
}

   span_dir.finish();

}
None =>{
}
}   

    }
}

//соединение с базой агрегации метрик
//let mut conn = Client::connect("postgresql://user:pass@host:5432/db", NoTls).unwrap();



for (ekey, evalue) in &scores_end {

    let re_string = Regex::new(r"(.*)\s(.*)\s(\d+)$").unwrap();
    
    match re_string.captures(&ekey) {
        Some(caps_string) => {
        
            let _agent_string = caps_string.get(1).unwrap().as_str();         
            let agent_host_name = _agent_string.replace("hostname=", "");
            let agent_host_name = agent_host_name.replace("name=", "");
            let (_, agent_name) = agent_host_name.split_once(",").unwrap();

            let mut sum_file_size = String::from("0");
        if scores_size.contains_key(agent_name) {
                sum_file_size = scores_size.get(agent_name).expect("cannot make int to string").to_string();
        }
           
        
            let _path_string = caps_string.get(2).unwrap().as_str();
            let _files_inlog_string = caps_string.get(3).unwrap().as_str();
            let files_inlog_int: i64 = _files_inlog_string.parse().unwrap();
            let mut stream_type = "sli"; 

            
        //удаляем агентов, которые есть в текущем лог-файле -> подставляем только агентов с значением 0 для файлов на текущий запуск
        if hash_map_type.contains_key(agent_name) {
            hash_map_type.remove(agent_name);
        }  
        

        if  !_agent_string.contains("type=err"){

        if _agent_string.contains("name=fix_") {
                    stream_type = "fix";
            }
        else if _agent_string.contains("name=msc_") {
                stream_type = "msc";
            }
        else if _agent_string.contains("name=gprs_") {
            stream_type = "gprs";
           }
        else if _agent_string.contains("name=sms_") {
            stream_type = "sms";
           }
        }  
         
        //let re_symbols = Regex::new(r"[\-]").unwrap();   
        //let re_agent_string = re_symbols.replace_all(_agent_string, "_");
        

        if in_out_key != 4 {
        
        if key1 == 1 && evalue == &0 {
            if !pitstop {
                sli = 100.0;
            }
                print_data = "q".to_owned() +&hostname+ "," +_agent_string+ ",dir=" +_path_string+ " ok=" +&files_inlog_int.to_string()+ ",bad=" +&evalue.to_string()+ ",size="+&sum_file_size+ "," +stream_type+ "=" +&sli.to_string();
                output_string.push(String::from(&print_data)); 

                //fs::write("/app/obss_log.txt", &print_data).expect("Unable to write file");
            //println!("q{},{}={} ok={}i,bad={}i,{}=100",hostname, _agent_string, _path_string, files_inlog_int, evalue, stream_type);
            }
        if key1 == 1 &&  evalue > &0 {
            
         if evalue > &files_inlog_int {
            if !pitstop{
                sli = 0.0;
            }
                print_data = "q".to_owned() +&hostname+ "," +_agent_string+ ",dir=" +_path_string+ " ok=" +&files_inlog_int.to_string()+ ",bad=" +&evalue.to_string()+ ",size="+&sum_file_size+ "," +stream_type+ "=" +&sli.to_string();
                output_string.push(String::from(&print_data));
               // let _ =  conn.execute("INSERT INTO aggregation (host, hostname, name, dir, bad, type) values ($1, $2, $3, $4, $5, $6) ON CONFLICT (dir) DO NOTHING", &[&hostname, &agent_host, &agent_name, &_path_string, evalue, &stream_type],); 
                //fs::write("/app/obss_log.txt", &print_data).expect("Unable to write file");
                //println!("{}", &print_data);
           // println!("q{},{}={} ok={}i,bad={}i,{}=0",hostname, _agent_string, _path_string, files_inlog_int, evalue, stream_type); 
          }
        else{       

            if !pitstop{
                    sli = 100.0 - (*evalue as f32 / files_inlog_int as f32 * 100.0);
            }        

                print_data = "q".to_owned() +&hostname+ "," +_agent_string+ ",dir=" +_path_string+ " ok=" +&files_inlog_int.to_string()+ ",bad=" +&evalue.to_string()+ ",size="+&sum_file_size+ "," +stream_type+ "=" +&sli.to_string();
                output_string.push(String::from(&print_data));
               // let _ =  conn.execute("INSERT INTO aggregation (host, hostname, name, dir, bad, type) values ($1, $2, $3, $4, $5, $6) ON CONFLICT (dir) DO NOTHING", &[&hostname, &agent_host, &agent_name, &_path_string, evalue, &stream_type],); 
                //fs::write("/app/obss_log.txt", &print_data).expect("Unable to write file");
                //println!("{}", &print_data);
            //println!("q{},{}={} ok={}i,bad={}i,{}={}",hostname, _agent_string, _path_string, files_inlog_int, evalue, stream_type, sli); 
        }  
         

        }
                      
            
        if key1 == 2 && evalue > &0 {
            print_data = "q".to_owned() +&hostname+ "," +_agent_string+ ",dir=" +_path_string+ " ok=" +&files_inlog_int.to_string()+ ",bad=" +&evalue.to_string()+ ",size="+&sum_file_size+ "," +stream_type+ "=0";
            output_string.push(String::from(&print_data));
           // let _ =  conn.execute("INSERT INTO aggregation (host, hostname, name, dir, bad, type) values ($1, $2, $3, $4, $5, $6) ON CONFLICT (dir) DO NOTHING", &[&hostname, &agent_host, &agent_name, &_path_string, evalue, &stream_type],); 
            //fs::write("/app/obss_log.txt", &print_data).expect("Unable to write file");
            //println!("{}", &print_data);
            //println!("{},{}={} ok={}i,bad={}i", hostname, _agent_string, _path_string, files_inlog_int, evalue);
                }
                
            } 
            
        if in_out_key == 4{
            print_data = "q".to_owned() +&hostname+ "," +_agent_string+ ",dir=" +_path_string+ " ok=" +&files_inlog_int.to_string()+ "i,bad=0,size="+&sum_file_size+ ","+stream_type+ "=0";
            output_string.push(String::from(&print_data));
            //fs::write("/app/obss_log.txt", &print_data).expect("Unable to write file");
            //println!("{}", &print_data);

            //println!("q{},{}={} ses={}i", hostname, _agent_string, _path_string, files_inlog_int);
        }


        }    
        None =>{
        }
}

//break;   
}


/*
for (name, _) in &hash_map_type {
    
    let agent_ftpserver = hash_map_server.get(name).unwrap_or(&binding);
    let agent_srcdir = hash_map_srcdir.get(name).unwrap_or(&binding);     
    let agent_dstdir = hash_map_dstdir.get(name).unwrap_or(&binding);

if agent_srcdir != "/dev/null/" {
    print_data = "q".to_owned() +&hostname+ ",hostname=" +agent_ftpserver+ ",name=" +name+ ",dir=" +agent_srcdir+ " ok=0,bad=0,sli=99.99";
    output_string.push(String::from(&print_data));
}
if agent_dstdir != "/dev/null/" {
    print_data = "q".to_owned() +&hostname+ ",hostname=" +agent_ftpserver+ ",name=" +name+ ",dir=" +agent_dstdir+ " ok=0,bad=0,sli=99.99";
    output_string.push(String::from(&print_data));
}

}
*/

//let error_agent_ftpserver = hash_map_server.get(a_name_slice).unwrap_or(&binding);
//println!("{:?}", hash_map_type);


output_string.sort();

transaction.finish();

output_string


}

fn main(){
    let duration_since_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let timestamp_nanos = duration_since_epoch.as_nanos(); // u128

for line in log_parser() {
    print!("{} {}\n", line, timestamp_nanos);
}


}

