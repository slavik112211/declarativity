module ChatProtocol
  state do
    channel :connect, [:@addr, :client] => [:nick]
    channel :mcast
  end

  DEFAULT_ADDR = "localhost:12345"

  # format chat messages with color and timestamp on the right of the screen
  def pretty_print(val)
    str = "\033[34m"+val[1].to_s + ": " + "\033[31m" + (val[3].to_s || '') + "\033[0m"
    pad = "(" + val[2].to_s + ")"
    return str + " "*[66 - str.length,2].max + pad
  end
end
