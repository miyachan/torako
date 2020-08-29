use crate::imageboard::Post;

pub(super) struct Daily {
    pub(super) day: u64,
    pub(super) posts: u64,
    pub(super) images: u64,
    pub(super) sage: u64,
    pub(super) anons: u64,
    pub(super) trips: u64,
    pub(super) names: u64,
}

impl Daily {
    pub(super) fn new(day: u64) -> Self {
        Self {
            day,
            posts: 0,
            images: 0,
            sage: 0,
            anons: 0,
            trips: 0,
            names: 0,
        }
    }

    pub(super) fn update(&mut self, post: &Post) {
        self.posts += 1;
        if post.filename.is_some() {
            self.images += 1;
        }
        if post.email.as_ref().map(|e| e == "sage").unwrap_or(false) {
            self.sage += 1;
        }
        if post
            .name
            .as_ref()
            .map(|n| n == "Anonymous")
            .unwrap_or(false)
            && post.trip.is_none()
        {
            self.anons += 1;
        }
        if post.trip.is_some() {
            self.trips += 1;
        }
        if post
            .name
            .as_ref()
            .map(|n| n != "Anonymous" && !n.is_empty())
            .unwrap_or(false)
        {
            self.names += 1;
        }
    }
}

pub(super) struct User {
    pub(super) name: String,
    pub(super) trip: String,
    pub(super) first_seen: u64,
    pub(super) post_count: i64,
}

impl User {
    pub(super) fn new(name: Option<String>, trip: Option<String>) -> Self {
        Self {
            name: name.unwrap_or(String::from("")),
            trip: trip.unwrap_or(String::from("")),
            first_seen: 0,
            post_count: 0,
        }
    }

    pub(super) fn update(&mut self, post: &Post) {
        self.post_count += 1;
        self.first_seen = self.first_seen.min(post.nyc_timestamp());
    }
}
