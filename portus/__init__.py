from langchain_core.language_models.chat_models import BaseChatModel
import logging
from portus.session import Session, SessionImpl

logger = logging.getLogger(__name__)
# Attach a NullHandler so importing apps without logging config don’t get warnings.
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


def create_session(llm: BaseChatModel) -> Session:
    return SessionImpl(llm)
