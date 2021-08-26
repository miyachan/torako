use serde::Serialize;

#[derive(Serialize, Clone, Debug)]
pub struct Post<'a> {
    board: &'static str,
    thread_no: u64,
    post_no: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tripcode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unique_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    since4_pass: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image_hash: Option<&'a str>,
    image_width: u64,
    image_height: u64,
    ts: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    comment: Option<String>,
    deleted: u64,
    ghost: u64,
    sticky: u64,
    spoiler: u64,
    op: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    capcode: Option<u64>,
}

impl<'a> From<&'a crate::imageboard::Post> for Post<'a> {
    fn from(post: &'a crate::imageboard::Post) -> Self {
        Post {
            board: post.board,
            thread_no: post.thread_no(),
            post_no: post.no,
            subject: post.sub.as_ref().map(|x| &**x),
            username: post.name.as_ref().map(|x| &**x),
            tripcode: post.trip.as_ref().map(|x| &**x),
            email: post.email.as_ref().map(|x| &**x),
            unique_id: post.id.as_ref().map(|x| &**x),
            since4_pass: post.since4pass.map(|s| s as u64),
            country: post.poster_country(),
            filename: post.media_filename(),
            image_hash: post.md5.as_ref().map(|x| &**x),
            image_width: post.w as _,
            image_height: post.h as _,
            ts: post.time as _,
            comment: post.comment(),
            deleted: if post.deleted { 1 } else { 0 },
            ghost: 0,
            sticky: if post.sticky { 1 } else { 0 },
            spoiler: if post.spoiler { 1 } else { 0 },
            op: if post.is_op() { 1 } else { 0 },
            capcode: post.short_capcode().chars().next().map(|c| c as u64),
        }
    }
}
