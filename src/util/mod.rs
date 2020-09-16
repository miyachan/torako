use clap::ArgMatches;

pub mod interval_lock;
pub mod pgs;

pub fn boo<'a>(_: &ArgMatches<'a>) -> i32 {
    println!("boo");
    return 0;
}
