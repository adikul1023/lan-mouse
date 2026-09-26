fn main() {
    let mut files: Vec<String> = Vec::new();
    let res = clipboard_win::get_clipboard::<Vec<String>, _>(clipboard_win::formats::FileList);
    println!("read result: {:?}", res);
    
    // writing
    // let files = vec!["C:\\Windows\\System32\\cmd.exe".to_string()];
    // clipboard_win::set_clipboard(clipboard_win::formats::FileList, files).unwrap();
}
