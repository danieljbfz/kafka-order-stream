import logging

class EmojiFormatter(logging.Formatter):
    """Associates specific emojis to different log levels"""
    LEVEL_EMOJIS = {
        logging.INFO: "🟢",
        logging.WARNING: "🟡",
        logging.ERROR: "🔴",
        logging.CRITICAL: "💀"
    }

    def format(self, record):
        emoji = self.LEVEL_EMOJIS.get(record.levelno, "📝")
        # Prepend emoji to the message
        record.msg = f"{emoji} {record.msg}"
        return super().format(record)