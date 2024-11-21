local json = require("json")

local function print_comparison(file1, file2)
    -- Get the column headers
    local headers = {"metrics", "left result", "right result" }

    -- Get the row data
    local rows = {
      {"Success value", file1.metrics.success.value, file2.metrics.success.value},
      {"Passes", file1.metrics.success.passes, file2.metrics.success.passes},
      {"Fails", file1.metrics.success.fails, file2.metrics.success.fails},
      {"VUs", file1.metrics.vus.value, file2.metrics.vus.value},
      {"Iterations rps", file1.metrics.iterations.rate, file2.metrics.iterations.rate}
    }

    -- Calculate the maximum length of each column
    local max_lengths = {0, 0, 0}
    for i, header in ipairs(headers) do
      max_lengths[i] = #header
    end
    for _, row in ipairs(rows) do
      for i, value in ipairs(row) do
        max_lengths[i] = math.max(max_lengths[i], #tostring(value))
      end
    end

    -- Print the table
    local separator = "+" .. string.rep("-", max_lengths[1] + 2)
      .. "+" .. string.rep("-", max_lengths[2] + 2)
      .. "+" .. string.rep("-", max_lengths[3] + 2) .. "+"
    print(separator)
    for i, header in ipairs(headers) do
      io.write("| " .. string.format("%-" .. max_lengths[i] .. "s", header) .. " ")
    end
    io.write("|\n")
    print(separator)
    for _, row in ipairs(rows) do
      for i, value in ipairs(row) do
        io.write("| " .. string.format("%-" .. max_lengths[i] .. "s", tostring(value)) .. " ")
      end
      io.write("|\n")
    end
    print(separator)
  end

local function load_file(file_path)
    local f = assert(io.open(file_path, "r"))
    local content = f:read("*all")
    f:close()
    return content
end

local function compare_iterations_rate(file_path1, file_path2)
    local content1 = load_file(file_path1)
    local content2 = load_file(file_path2)
    local data1 = json.decode(content1)
    local data2 = json.decode(content2)
    local rate1 = data1.metrics.iterations.rate
    local rate2 = data2.metrics.iterations.rate

    print_comparison(data1, data2, file_path1, file_path2)
    if rate1 >= rate2 then
        print("Left (first) result must have lower or equal rps")
        os.exit(1)
    end
    os.exit(0)
end

compare_iterations_rate(arg[1], arg[2])
