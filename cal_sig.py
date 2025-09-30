def truncate_utf8_with_ellipsis(text: str, max_bytes: int = 63) -> str:
    """
    Truncate a string so its UTF-8 encoding does not exceed max_bytes.
    Append '...' if truncated.
    Ensures we don't cut in the middle of a character.
    """
    utf8_bytes = text.encode("utf-8")
    if len(utf8_bytes) <= max_bytes:
        return text

    # Reserve space for the ellipsis
    ellipsis = "...".encode("utf-8")
    limit = max_bytes - len(ellipsis)

    truncated = utf8_bytes[:limit]
    while True:
        try:
            return truncated.decode("utf-8") + "..."
        except UnicodeDecodeError:
            truncated = truncated[:-1]  # remove last byte until valid


# --- Example usage ---
name1 = "สิริวุฒิ กิตติโพธินันท์"
name2 = "มูฮำหมัดรอมฎอน อูมานาสารา"
name3 = "น้องทิพย์ ฐิติรัตน์ พรมแต่ง"
name4 = "ชญาภา ตันศิรินาถกุล"
name5 = "Mam Chuenteerawong"
name6= "หนึ่งฤทัย สงวนศักดิ์ภักดี"

names = [ name1 , name2 , name3 , name4 , name5 ,name6]


for name in names : 

    print("Original:", name, len(name.encode("utf-8")))
    print("Result:  ", truncate_utf8_with_ellipsis(name, 58))