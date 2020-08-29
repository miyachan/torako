use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::SystemTime;

use chrono::{NaiveDateTime, TimeZone};
use chrono_tz::America::New_York;
use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};
use serde::de::{self, Deserializer, Unexpected};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Clone)]
pub struct CatalogPage {
    pub page: usize,
    pub threads: Vec<CatalogThread>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CatalogThread {
    #[serde(default, skip)]
    pub page: usize,
    #[serde(default, skip)]
    pub board: smallstr::SmallString<[u8; 4]>,
    pub no: u64,
    pub resto: u64,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub sticky: bool,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub closed: bool,
    pub now: String,
    pub time: i64,
    pub name: Option<String>,
    pub trip: Option<String>,
    pub id: Option<String>,
    pub capcode: Option<String>,
    pub country: Option<String>,
    pub sub: Option<String>,
    pub com: Option<String>,
    pub tim: Option<u64>,
    pub filename: Option<String>,
    pub ext: Option<String>,
    pub fsize: Option<u64>,
    pub md5: Option<String>,
    pub w: Option<u32>,
    pub h: Option<u32>,
    pub tn_w: Option<u32>,
    pub tn_h: Option<u32>,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub file_deleted: bool,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub spoiler: bool,
    #[serde(default)]
    pub custom_spoiler: u8,
    #[serde(default)]
    pub omitted_posts: u16,
    #[serde(default)]
    pub omitted_images: u16,
    #[serde(default)]
    pub replies: u16,
    #[serde(default)]
    pub images: u16,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub bumplimit: bool,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub imagelimit: bool,
    pub last_modified: u64,
    pub tag: Option<String>,
    pub semantic_url: Option<String>,
    pub since4pass: Option<u32>,
    #[serde(default)]
    pub unique_ips: u32,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub m_img: bool,
    #[serde(default)]
    pub last_replies: Vec<Post>,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub struct Thread {
    pub posts: Vec<Post>,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub struct Post {
    #[serde(default, skip)]
    pub board: smallstr::SmallString<[u8; 4]>,
    pub no: u64,
    pub resto: u64,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub sticky: bool,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub closed: bool,
    pub now: String,
    pub time: u64,
    pub name: Option<String>,
    pub email: Option<String>,
    pub trip: Option<String>,
    pub id: Option<String>,
    pub capcode: Option<String>,
    pub country: Option<String>,
    pub country_name: Option<String>,
    pub troll_country: Option<String>, // ?
    pub sub: Option<String>,
    pub com: Option<String>,
    pub tim: Option<u64>,
    pub filename: Option<String>,
    pub ext: Option<String>,
    #[serde(default)]
    pub fsize: u64,
    pub md5: Option<String>,
    #[serde(default)]
    pub w: u32,
    #[serde(default)]
    pub h: u32,
    #[serde(default)]
    pub tn_w: u32,
    #[serde(default)]
    pub tn_h: u32,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub file_deleted: bool,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub spoiler: bool,
    #[serde(default)]
    pub custom_spoiler: u8,
    #[serde(default)]
    pub replies: u16,
    #[serde(default)]
    pub images: u16,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub bumplimit: bool,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub imagelimit: bool,
    pub tag: Option<String>,
    pub semantic_url: Option<String>,
    pub since4pass: Option<u32>,
    #[serde(default)]
    pub unique_ips: u32,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub m_img: bool,
    #[serde(default, deserialize_with = "bool_from_int")]
    pub archived: bool,
    pub archived_on: Option<u64>,
    #[serde(default, skip)]
    pub deleted: bool,
    #[serde(default, skip)]
    pub deleted_at: Option<u64>,
    #[serde(default, skip)]
    pub is_retransmission: bool,
}

impl Post {
    pub fn is_op(&self) -> bool {
        self.resto == 0
    }

    pub fn thread_no(&self) -> u64 {
        if self.resto == 0 {
            self.no
        } else {
            self.resto
        }
    }

    pub fn nyc_timestamp(&self) -> i64 {
        let ny_time = chrono::Utc
            .timestamp(self.time as i64, 0)
            .with_timezone(&New_York);
        ny_time.naive_local().timestamp()
    }

    pub fn datetime(&self) -> chrono::NaiveDateTime {
        NaiveDateTime::from_timestamp(self.time as i64, 0)
    }

    pub fn preview_orig(&self) -> Option<String> {
        self.tim.as_ref().map(|t| format!("{}s.jpg", t))
    }

    pub fn media_filename(&self) -> Option<String> {
        self.filename
            .as_ref()
            .map(|x| x.to_string() + self.ext.as_ref().unwrap())
    }

    pub fn media_orig(&self) -> Option<String> {
        self.tim
            .as_ref()
            .zip(self.ext.as_ref())
            .map(|(tim, ext)| format!("{}{}", tim, ext))
    }

    pub fn short_capcode(&self) -> String {
        self.capcode
            .as_ref()
            .map(|c| match c.to_ascii_lowercase().as_str() {
                "manager" => String::from("M"),
                _ => c.chars().next().unwrap().to_ascii_uppercase().to_string(),
            })
            .unwrap_or(String::from("N"))
    }

    pub fn poster_name(&self) -> Option<String> {
        self.name.as_ref().map(|x| Self::clean_simple(x))
    }

    pub fn poster_nametrip(&self) -> Option<String> {
        match (self.name.as_ref(), self.trip.as_ref()) {
            (Some(name), Some(trip)) => Some(format!("{}{}", name, trip)),
            (Some(name), _) => Some(name.clone()),
            (_, Some(trip)) => Some(trip.clone()),
            _ => None,
        }
    }

    pub fn title(&self) -> Option<String> {
        self.sub.as_ref().map(|x| Self::clean_simple(x))
    }

    pub fn poster_hash(&self) -> Option<String> {
        self.id.as_ref().map(|x| {
            if x.as_str() == "Developer" {
                String::from("Dev")
            } else {
                x.to_string()
            }
        })
    }

    pub fn poster_country(&self) -> Option<String> {
        match &self.country {
            Some(country) => match country.as_ref() {
                "XX" | "A1" => None,
                _ => Some(country.clone()),
            },
            None => None,
        }
    }

    pub fn comment(&self) -> Option<String> {
        self.com.as_ref().map(|x| Post::clean_heavy(x.as_str()))
    }

    pub fn comment_hash(&self) -> u64 {
        match self.com.as_ref() {
            Some(t) => {
                let mut s = fnv::FnvHasher::default();
                t.hash(&mut s);
                s.finish()
            }
            None => 0,
        }
    }

    pub fn has_media(&self) -> bool {
        self.filename.is_some()
    }

    pub fn exif(&self) -> Option<String> {
        let exif = Exif::parse(&self);
        if exif.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&exif).unwrap())
        }
    }

    pub fn clean_simple<T: AsRef<str>>(text: T) -> String {
        match htmlescape::decode_html(text.as_ref()) {
            Ok(text) => text.replace("\\s*$", "").replace("^\\s*$", ""),
            Err(err) => {
                log::warn!("error: {:?}", err);
                text.as_ref()
                    .replace("&gt;", ">")
                    .replace("&lt;", "<")
                    .replace("&quot;", "\"")
                    .replace("&amp;", "&")
                    .replace("\\s*$", "")
                    .replace("^\\s*$", "")
            }
        }
    }

    pub fn clean_heavy<T: AsRef<str>>(text: T) -> String {
        lazy_static! {
            static ref RE_PIPELINE: [(Regex, &'static str); 23] = [
                // Admin-Mod-Dev quotelinks
                (Regex::new("<span class=\"capcodeReplies\"><span style=\"font-size: smaller;\"><span style=\"font-weight: bold;\">(?:Administrator|Moderator|Developer) Repl(?:y|ies):</span>.*?</span><br></span>").unwrap(), ""),
                // Non-public tags
                (Regex::new("\\[(/?(banned|moot|spoiler|code))]").unwrap(), "[$1:lit]"),
                // Comment too long, also EXIF tag toggle
                (Regex::new("<span class=\"abbr\">.*?</span>").unwrap(), ""),
                // EXIF data
                (Regex::new("<table class=\"exif\"[^>]*>.*?</table>").unwrap(), ""),
                // DRAW data
                (Regex::new("<br><br><small><b>Oekaki Post</b>.*?</small>").unwrap(), ""),
                // Banned/Warned text
                (Regex::new("<(?:b|strong) style=\"color:\\s*red;\">(.*?)</(?:b|strong)>").unwrap(), "[banned]$1[/banned]"),
                // moot text
                (Regex::new("<div style=\"padding: 5px;margin-left: \\.5em;border-color: #faa;border: 2px dashed rgba\\(255,0,0,\\.1\\);border-radius: 2px\">(.*?)</div>").unwrap(), "[moot]$1[/moot]"),
                // fortune text
                (Regex::new("<span class=\"fortune\" style=\"color:(.*?)\"><br><br><b>(.*?)</b></span>").unwrap(), "\n\n[fortune color=\"$1\"]$2[/fortune]"),
                // bold text
                (Regex::new("<(?:b|strong)>(.*?)</(?:b|strong)>").unwrap(), "[b]$1[/b]"),
                // code tags
                (Regex::new("<pre[^>]*>").unwrap(), "[code]"),
                (Regex::new("</pre>").unwrap(), "[/code]"),
                // math tags
                (Regex::new("<span class=\"math\">(.*?)</span>").unwrap(), "[math]$1[/math]"),
                (Regex::new("<div class=\"math\">(.*?)</div>").unwrap(), "[eqn]$1[/eqn]"),
                // > implying I'm quoting someone
                (Regex::new("<font class=\"unkfunc\">(.*?)</font>").unwrap(), "$1"),
                (Regex::new("<span class=\"quote\">(.*?)</span>").unwrap(), "$1"),
                (Regex::new("<span class=\"(?:[^\"]*)?deadlink\">(.*?)</span>").unwrap(), "$1"),
                // Links
                (Regex::new("<a[^>]*>(.*?)</a>").unwrap(), "$1"),
                // old spoilers
                (Regex::new("<span class=\"spoiler\"[^>]*>(.*?)</span>").unwrap(), "[spoiler]$1[/spoiler]"),
                // ShiftJIS
                (Regex::new("<span class=\"sjis\">(.*?)</span>").unwrap(), "[shiftjis]$1[/shiftjis]"),
                // new spoilers
                (Regex::new("<s>").unwrap(), "[spoiler]"),
                (Regex::new("</s>").unwrap(), "[/spoiler]"),
                // new line/wbr
                (Regex::new("<br\\s*/?>").unwrap(), "\n"),
                (Regex::new("<wbr>").unwrap(), "")
            ];
        }

        let mut ret = String::from(text.as_ref());
        for (patt, repl) in RE_PIPELINE.iter() {
            match patt.replace_all(&ret, *repl) {
                std::borrow::Cow::Owned(s) => {
                    ret.clear();
                    ret.push_str(s.as_ref());
                }
                std::borrow::Cow::Borrowed(_) => (),
            };
        }

        ret
    }

    pub fn deleted<T: AsRef<str>>(board: T, no: u64) -> Self {
        Self {
            board: smallstr::SmallString::from_str(board.as_ref()),
            no,
            is_retransmission: true,
            deleted: true,
            deleted_at: match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(n) => Some(n.as_secs()),
                Err(_) => panic!("SystemTime before UNIX EPOCH!"),
            },
            ..Default::default()
        }
    }
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct Exif {
    #[serde(rename = "uniqueIps", skip_serializing_if = "Option::is_none")]
    unique_ips: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    since4pass: Option<String>,
    #[serde(rename = "trollCountry", skip_serializing_if = "Option::is_none")]
    troll_country: Option<String>,
    #[serde(rename = "Time", skip_serializing_if = "Option::is_none")]
    time: Option<String>,
    #[serde(rename = "Painter", skip_serializing_if = "Option::is_none")]
    painter: Option<String>,
    #[serde(rename = "Source", skip_serializing_if = "Option::is_none")]
    source: Option<String>,

    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    exif_data: HashMap<String, String>,
}

impl Exif {
    fn parse(post: &Post) -> Self {
        lazy_static! {
            static ref DRAW_RE: Regex = RegexBuilder::new("<small><b>Oekaki \\s Post</b> \\s \\(Time: \\s (.*?), \\s Painter: \\s (.*?)(?:, \\s Source: \\s (.*?))?(?:, \\s Animation: \\s (.*?))?\\)</small>").dot_matches_new_line(true).ignore_whitespace(true).build().unwrap();
            static ref EXIF_RE: Regex = RegexBuilder::new("<table \\s class=\"exif\"[^>]*>(.*)</table>").dot_matches_new_line(true).ignore_whitespace(true).build().unwrap();
            static ref EXIF_DATA_RE: Regex = RegexBuilder::new("<tr><td>(.*?)</td><td>(.*?)</td></tr>").dot_matches_new_line(true).ignore_whitespace(true).build().unwrap();
        }

        let mut exif_data = HashMap::new();
        let mut time = None;
        let mut painter = None;
        let mut source = None;
        if let Some(text) = post.com.as_ref() {
            if let Some(exif) = EXIF_RE.captures(text.as_str()) {
                let data = exif[1].replace("<tr><td colspan=\"2\"></td></tr><tr>", "");
                for cap in EXIF_DATA_RE.captures_iter(&data) {
                    exif_data.insert(String::from(&cap[1]), String::from(&cap[2]));
                }
            }
            if let Some(draw) = DRAW_RE.captures(text.as_str()) {
                time = Some(String::from(&draw[1]));
                painter = Some(String::from(&draw[2]));
                source = draw.get(3).map(|source| Post::clean_heavy(source.as_str()))
            }
        }

        Self {
            unique_ips: if post.unique_ips == 0 {
                None
            } else {
                Some(post.unique_ips.to_string())
            },
            since4pass: post
                .since4pass
                .map(|x| if x == 0 { None } else { Some(x.to_string()) })
                .flatten(),
            troll_country: post.troll_country.clone(),
            time,
            painter,
            source,
            exif_data,
        }
    }

    fn is_empty(&self) -> bool {
        self.unique_ips.is_none()
            && self.since4pass.is_none()
            && self.troll_country.is_none()
            && self.time.is_none()
            && self.painter.is_none()
            && self.source.is_none()
            && self.exif_data.is_empty()
    }
}

fn bool_from_int<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    match u8::deserialize(deserializer)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(de::Error::invalid_value(
            Unexpected::Unsigned(other as u64),
            &"zero or one",
        )),
    }
}
