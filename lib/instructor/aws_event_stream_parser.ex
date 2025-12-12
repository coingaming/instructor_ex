defmodule Instructor.AWSEventStreamParser do
  @moduledoc """
  Parser for AWS Event Stream binary format (application/vnd.amazon.eventstream).

  Used by AWS Bedrock ConverseStream and other AWS streaming APIs.

  ## Event Stream Format

  Each message in the stream has the following structure:
  - 4 bytes: total message length
  - 4 bytes: headers length
  - 4 bytes: prelude CRC32
  - Headers section (variable length)
  - Payload section (variable length)
  - 4 bytes: message CRC32

  ## Header Format

  Each header consists of:
  - 1 byte: header name length
  - N bytes: header name
  - 1 byte: header value type (7 = string)
  - For string type: 2 bytes length + N bytes value
  """

  # AWS Event Stream message structure constants
  @total_length_bytes 4
  @headers_length_bytes 4
  @prelude_crc_bytes 4
  @message_crc_bytes 4

  @prelude_bytes @total_length_bytes + @headers_length_bytes + @prelude_crc_bytes
  @fixed_overhead @prelude_bytes + @message_crc_bytes

  @doc """
  Parses a stream of binary chunks into decoded JSON events.

  Returns a stream of parsed JSON maps from the AWS Event Stream format.
  """
  @spec parse(stream :: Enumerable.t()) :: Enumerable.t()
  def parse(stream) do
    Stream.transform(
      stream,
      fn -> <<>> end,
      fn chunk, buf ->
        parse_events(buf <> chunk, [])
      end,
      fn buf ->
        case parse_events(buf, []) do
          {events, _rest} -> {events, <<>>}
        end
      end,
      fn _ -> nil end
    )
  end

  defp parse_events(<<>>, acc), do: {Enum.reverse(acc), <<>>}

  defp parse_events(data, acc) when byte_size(data) < @prelude_bytes do
    {Enum.reverse(acc), data}
  end

  defp parse_events(
         <<total_length::32, headers_length::32, _prelude_crc::32, rest::binary>> = data,
         acc
       ) do
    payload_length = total_length - headers_length - @fixed_overhead

    if byte_size(rest) >= headers_length + payload_length + @message_crc_bytes do
      <<headers_data::binary-size(headers_length), payload::binary-size(payload_length),
        _message_crc::32, remaining::binary>> = rest

      case parse_event_payload(headers_data, payload) do
        {:ok, event} ->
          parse_events(remaining, [event | acc])

        :skip ->
          parse_events(remaining, acc)

        {:error, _reason} ->
          parse_events(remaining, acc)
      end
    else
      {Enum.reverse(acc), data}
    end
  end

  defp parse_events(data, acc), do: {Enum.reverse(acc), data}

  defp parse_event_payload(headers_data, payload) do
    headers = parse_headers(headers_data, %{})
    event_type = headers[":event-type"]
    message_type = headers[":message-type"]

    cond do
      message_type == "exception" ->
        {:error, payload}

      # Accept all Bedrock Converse stream event types
      message_type == "event" and byte_size(payload) > 0 ->
        case Jason.decode(payload) do
          {:ok, %{"bytes" => base64_bytes}} ->
            {:ok, Jason.decode!(Base.decode64!(base64_bytes))}

          {:ok, json} ->
            {:ok, json}

          {:error, _} ->
            :skip
        end

      # Legacy: also accept "chunk" event type for backwards compatibility
      event_type in ["chunk", nil] and byte_size(payload) > 0 ->
        case Jason.decode(payload) do
          {:ok, %{"bytes" => base64_bytes}} ->
            {:ok, Jason.decode!(Base.decode64!(base64_bytes))}

          {:ok, json} ->
            {:ok, json}

          {:error, _} ->
            :skip
        end

      true ->
        :skip
    end
  end

  defp parse_headers(<<>>, acc), do: acc

  defp parse_headers(<<name_length::8, rest::binary>>, acc) do
    <<name::binary-size(name_length), type::8, rest::binary>> = rest

    {value, rest} =
      case type do
        type when type in [0, 1] -> {nil, rest}
        2 -> <<_::8, rest::binary>> = rest; {nil, rest}
        3 -> <<_::16, rest::binary>> = rest; {nil, rest}
        4 -> <<_::32, rest::binary>> = rest; {nil, rest}
        5 -> <<_::64, rest::binary>> = rest; {nil, rest}
        6 -> <<len::16, _::binary-size(len), rest::binary>> = rest; {nil, rest}
        7 -> <<len::16, value::binary-size(len), rest::binary>> = rest; {value, rest}
        8 -> <<_::64, rest::binary>> = rest; {nil, rest}
        9 -> <<_::binary-size(16), rest::binary>> = rest; {nil, rest}
      end

    parse_headers(rest, Map.put(acc, name, value))
  end
end
