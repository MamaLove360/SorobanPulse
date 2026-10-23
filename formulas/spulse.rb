class Spulse < Formula
  desc "Command-line tool for querying and analyzing Soroban Pulse events"
  homepage "https://github.com/soroban-pulse/soroban-pulse"
  version "0.1.0"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/soroban-pulse/soroban-pulse/releases/download/v#{version}/spulse-#{version}-aarch64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER_SHA256_AARCH64"
    else
      url "https://github.com/soroban-pulse/soroban-pulse/releases/download/v#{version}/spulse-#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER_SHA256_X86_64"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/soroban-pulse/soroban-pulse/releases/download/v#{version}/spulse-#{version}-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "PLACEHOLDER_SHA256_LINUX_ARM"
    else
      url "https://github.com/soroban-pulse/soroban-pulse/releases/download/v#{version}/spulse-#{version}-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "PLACEHOLDER_SHA256_LINUX_X86"
    end
  end

  def install
    bin.install "spulse"

    bash_completion.install "spulse.bash" => "spulse"
    zsh_completion.install "_spulse"
    fish_completion.install "spulse.fish"

    man1.install "spulse.1"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/spulse --version")
  end
end
