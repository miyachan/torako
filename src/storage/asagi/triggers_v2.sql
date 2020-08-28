DROP TRIGGER IF EXISTS `before_ins_%%BOARD%%`;
DROP TRIGGER IF EXISTS `after_ins_%%BOARD%%`;

DROP PROCEDURE IF EXISTS `delete_thread_%%BOARD%%`;

CREATE PROCEDURE `delete_thread_%%BOARD%%` (tnum INT)
BEGIN
  DELETE FROM `%%BOARD%%_threads` WHERE thread_num = tnum;
END;

DROP PROCEDURE IF EXISTS `delete_image_%%BOARD%%`;

CREATE PROCEDURE `delete_image_%%BOARD%%` (n_media_id INT)
BEGIN
  UPDATE `%%BOARD%%_images` SET total = (total - 1) WHERE media_id = n_media_id;
END;


DROP TRIGGER IF EXISTS `after_del_%%BOARD%%`;

CREATE TRIGGER `after_del_%%BOARD%%` AFTER DELETE ON `%%BOARD%%`
FOR EACH ROW
BEGIN
  CALL update_thread_%%BOARD%%(OLD.thread_num, OLD.subnum, OLD.timestamp, OLD.media_hash, OLD.email);
  IF OLD.op = 1 THEN
    CALL delete_thread_%%BOARD%%(OLD.num);
  END IF;
  IF OLD.media_hash IS NOT NULL THEN
    CALL delete_image_%%BOARD%%(OLD.media_id);
  END IF;
END;