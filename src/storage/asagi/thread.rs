use crate::imageboard::Post;

#[derive(Debug, Clone)]
pub(super) struct Thread {
    pub(super) thread_num: u64,
    pub(super) time_op: Option<u64>,
    pub(super) time_last: u64,
    pub(super) time_bump: u64,
    pub(super) time_ghost: u64,
    pub(super) time_ghost_bump: u64,
    pub(super) time_last_modified: u64,
    pub(super) n_replies: u64,
    pub(super) n_images: u64,
    pub(super) sticky: Option<bool>,
    pub(super) locked: Option<bool>,
}

impl Thread {
    pub(super) fn new(thread_num: u64) -> Self {
        Self {
            thread_num,
            time_op: None,
            time_last: 0,
            time_bump: 0,
            time_ghost: 0,
            time_ghost_bump: 0,
            time_last_modified: 0,
            n_replies: 0,
            n_images: 0,
            sticky: None,
            locked: None,
        }
    }

    pub(super) fn update(&mut self, post: &Post) {
        let post_timestamp = post.nyc_timestamp() as u64;
        if post.is_op() {
            self.time_op = Some(post_timestamp);
            // self.sticky = Some(post.sticky);
            // self.locked = Some(post.closed);
        }
        self.time_last = self.time_last.max(post_timestamp);
        self.time_last_modified = self.time_last;
        if post.email.as_ref().map(|e| e != "sage").unwrap_or(true) {
            self.time_bump = self.time_bump.max(post_timestamp);
        }

        self.n_replies += 1;
        self.n_images += match post.filename {
            Some(_) => 1,
            None => 0,
        };
    }
}

#[derive(Debug, Clone)]
pub(super) struct Media {
    pub(super) media_id: u64,
    pub(super) media_hash: String,
    pub(super) media: Option<String>,
    pub(super) preview_op: Option<String>,
    pub(super) preview_reply: Option<String>,
    pub(super) total: u64,
    pub(super) banned: bool,
}
