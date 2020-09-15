use clap::ArgMatches;

pub mod pgs;

pub fn boo<'a>(_: &ArgMatches<'a>) -> i32 {
    println!("boo");
    return 0;
}
