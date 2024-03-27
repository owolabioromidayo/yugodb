use std::thread;
use std::net::{TcpListener, TcpStream, Shutdown};
use std::io::{Read, Write};

use yugodb::tokenizer; 

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0 as u8; 1024]; 
    stream.read(&mut buffer).unwrap();

    let mut resp_buffer = [0 as u8; 1024]; 
    let resp = "Thank you!";

    resp_buffer[..resp.len()].copy_from_slice(resp.as_bytes());
    

    match std::str::from_utf8(&buffer) {
        Ok(data) => {
            println!("Decoded UTF-8 string: {}", data);
            stream.write(&resp_buffer).unwrap();
            
        }
        Err(e) => {
            println!("Error decoding byte string: {}", e);
            stream.shutdown(Shutdown::Both).unwrap();
            
        }
    }
}

fn main() {
    let listener = TcpListener::bind("0.0.0.0:3333").unwrap();
    // accept connections and process them, spawning a new thread for each one
    println!("Server listening on port 3333");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("New connection: {}", stream.peer_addr().unwrap());
                thread::spawn(move|| {
                    handle_client(stream)
                });
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }
    drop(listener);
}
