package util

object ConsoleOps {

  implicit class Colorized(val str: String) extends AnyVal {

    import Console._

    def red = s"$RED$str"

    def green = s"$GREEN$str"

    def yellow = s"$YELLOW$str"

    def blue = s"$BLUE$str"

    def magenta = s"$MAGENTA$str"

    def cyan = s"$CYAN$str"

    def white = s"$WHITE$str"

    // Aliases

    def info: String = blue

    def err: String = red

    def warn: String = yellow

    def ok: String = green

  }

}
