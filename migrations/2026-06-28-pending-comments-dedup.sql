CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_comments_client_posturl
  ON pending_comments (client_id, post_url)
  WHERE post_url IS NOT NULL AND status = 'pending';
