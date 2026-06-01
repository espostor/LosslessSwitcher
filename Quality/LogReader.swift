//
//  LogReader.swift
//  LosslessSwitcher
//
//  Created by Vincent Neo on 1/3/26.
//

import Foundation
import Combine
import Sweep

// Sample-rate signals in the macOS unified log (as of macOS 26). If a future
// OS release breaks rate detection, this is the map of what's available:
//
//  1. com.apple.Music:ampplay — "asbdSampleRate = N kHz"
//     The RENDERED rate Music outputs; it tracks the output device's *current*
//     rate, so a hi-res track on a device sitting at a lower rate reads low.
//     Upstream v3 uses this as its only source, which is why it never climbs to
//     hi-res once the device has been pulled down. We keep it only as a fallback
//     and for the track name (the activeFormat line below has no title).
//
//  2. com.apple.amp.mediaplaybackcore:PlaybackEvents —
//     "activeFormat: tier: ...; groupID: audio-alac-stereo-96000-24; ..."
//     The track's INTENDED lossless format (true source rate + depth),
//     independent of what got rendered. This is our authoritative source; see
//     processActiveFormatLine.
//
//  3. com.apple.coreaudio:ac — "ACAppleLosslessDecoder.cpp ... Input format:
//     2 ch, 96000 Hz, alac ... from 24-bit source"
//     Also a true source rate (the decoder's input). This is what upstream read
//     pre-v3 ("use only coreaudio log", commit d782922). Authoritative but has
//     no track name and is very chatty (~1600 lines/15min). Documented as a
//     fallback if (2) ever stops working.
class LogReader {

    let entryStream = PassthroughSubject<CMEntry, Never>()

    private var process: Process?
    private let dateFormatter: DateFormatter

    // The `ampplay` line carries the track name and is logged immediately before
    // the `PlaybackEvents` activeFormat line for the same track, so we remember
    // the most recent name to attribute the activeFormat entry to it.
    private var lastTrackName: String?

    // The same activeFormat line is logged several times per track change;
    // collapse consecutive identical emissions.
    private var lastActiveFormatKey: String?

    // availableData can split mid-line; keep the trailing partial line here.
    private var buffer = ""

    init() {
        let dateFormatter = DateFormatter()
        dateFormatter.timeZone = .autoupdatingCurrent
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")
        dateFormatter.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
        self.dateFormatter = dateFormatter
    }

    func spawnProcessIfNeeded() {
        guard process == nil else { return }
        self.spawnProcess()
    }

    private func spawnProcess() {
        let process = Process()
        self.process = process

        process.executableURL = URL(filePath: "/usr/bin/log")
        process.arguments = [
            "stream",
            "--style",
            "compact",
            "--no-backtrace",
            "--predicate",
            // ampplay carries the rendered (possibly downsampled) rate + track name;
            // PlaybackEvents carries the track's *intended* lossless format.
            "process == \"Music\" AND (category == \"ampplay\" OR category == \"PlaybackEvents\")"
        ]

        let pipe = Pipe()
        process.standardOutput = pipe

        pipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty else {
                handle.readabilityHandler = nil
                return
            }

            guard let chunk = String(data: data, encoding: .utf8) else { return }
            self?.ingest(chunk)
        }

        do {
            try process.run()
        }
        catch {
            print("ProcessErr \(error)")
        }
    }

    // Reassembles complete log lines from arbitrarily-chunked reads, so a line
    // split across two reads (and cross-line ampplay → activeFormat correlation)
    // is handled correctly.
    private func ingest(_ chunk: String) {
        buffer += chunk
        while let newline = buffer.firstIndex(of: "\n") {
            let line = String(buffer[..<newline])
            buffer.removeSubrange(buffer.startIndex...newline)
            processLine(line)
        }
    }

    private func processLine(_ line: String) {
        if line.contains("[com.apple.Music:ampplay] play> cm>> ") {
            processAmpplayLine(line)
        }
        else if line.contains("activeFormat: tier: ") {
            processActiveFormatLine(line)
        }
    }

    // The original log source: the rate here (`asbdSampleRate`) is what Music
    // actually *renders*, which it picks to match the output device's current
    // rate — so for a hi-res track on a device that isn't already high, this is
    // the downsampled rate. We still emit it as a fallback (e.g. for sources that
    // don't log an activeFormat); when an activeFormat line follows, its true-rate
    // entry overwrites this one (same track key, InfoPair is a class) before the
    // throttled switch in OutputDevices fires, so hi-res settles on the real rate.
    private func processAmpplayLine(_ line: String) {
        guard let dateSubstring = line.firstSubstring(between: .start, and: " Df ") else { return }
        guard let messageContentSubstring = line.firstSubstring(between: "[com.apple.Music:ampplay] play> cm>> " , and: .end) else { return }
        let dateString = String(dateSubstring)
        let message = String(messageContentSubstring)
        let date = dateFormatter.date(from: dateString)

        let split = message.split(separator: ",")

        // The title is wrapped in single quotes and can itself contain
        // apostrophes (e.g. "I'm On Fire") or commas. Pull the whole thing out by
        // its delimiters rather than splitting on "," or stopping at the first
        // inner quote: otherwise the parsed name won't match MediaRemote's title,
        // the format and track entries land under different collection keys
        // (e.g. "I" vs "I'm On Fire") and never pair, so the device never switches.
        var trackName = message
            .firstSubstring(between: "mediaFormatinfo '", and: "' ,")
            .map { String($0).trimmingCharacters(in: .whitespaces) }
        var isLossless: Bool?
        var bitDepth: Int?
        var sampleRate: Int?

        for element in split {

            // notes: there is a field that may be "lossless", "high res lossless" and "stereo (lossy)"

            if isLossless == nil, element.hasPrefix(" sdFormatID") {
                guard let substring = element.firstSubstring(between: "= ", and: .end) else { continue }
                isLossless = substring == "alac"
                continue
            }

            if bitDepth == nil, element.hasPrefix(" sdBitDepth") {
                guard let substring = element.firstSubstring(between: "= ", and: " bit") else { continue }
                bitDepth = Int(substring)
            }

            if sampleRate == nil, element.hasPrefix(" asbdSampleRate") {
                guard let substring = element.firstSubstring(between: "= ", and: " kHz") else { continue }
                let string = String(substring)
                guard let double = Double(string) else { continue }
                sampleRate = Int(double * 1000)
                continue
            }

        }

        // this requires an external profile to read this info
        // might be helpful to prevent the early track sample rate switch issue.
        // https://eclecticlight.co/2023/03/08/removing-privacy-censorship-from-the-log/
        if let tn = trackName, tn == "<private>" {
            trackName = nil
        }

        // Remember the current track so the following activeFormat line can be
        // attributed to it (and merge into the same OutputDevices collection key).
        lastTrackName = trackName

        guard let date, let isLossless, let sampleRate else { return }

        // discard if entry is known to be for lossy playback
        // why?: while it could be nice to switch if you're playing a bunch of tracks where some are lossy,
        //       occassionally, there are lossless tracks where logs start off with these lossy information.
        //       to prevent over switching, i'm ignoring all lossy log entries.
        guard isLossless else { return }

        let entry = CMEntry(date: date, trackName: trackName, bitDepth: bitDepth, sampleRate: sampleRate)
        entryStream.send(entry)
    }

    // Apple Music logs the track's *intended* lossless format here — e.g. 96 kHz —
    // even when it renders/downsamples to a lower rate to match the current output
    // device. Reading this lets LosslessSwitcher drive the DAC up to the track's
    // true rate instead of chasing the already-downsampled `asbdSampleRate`,
    // breaking the chicken-and-egg where a low device rate keeps hi-res low.
    //
    // Example line:
    //   ... activeFormat: tier: HighResolutionLossless; groupID: audio-alac-stereo-96000-24;
    //       bitDepth: 24-bit; sampleRate: 96khz; codec: alac; channels: 2; ...
    //
    // Exact rate/depth are taken from the groupID token (audio-<codec>-<layout>-<rate>-<depth>)
    // rather than the rounded `sampleRate: 96khz` (which would give 44000, not 44100).
    private func processActiveFormatLine(_ line: String) {
        guard let dateSubstring = line.firstSubstring(between: .start, and: " Df ") else { return }
        guard let tier = line.firstSubstring(between: "activeFormat: tier: ", and: ";") else { return }

        // Lossy tiers (e.g. "HighQualityStereo", codec aac) carry no rate/depth in
        // their groupID; skip them, matching the lossless-only ampplay behaviour.
        guard String(tier).contains("Lossless") else { return }

        guard let groupID = line.firstSubstring(between: "groupID: ", and: ";") else { return }

        let parts = String(groupID).split(separator: "-")
        guard parts.count >= 2,
              let bitDepth = Int(parts[parts.count - 1]),
              let sampleRate = Int(parts[parts.count - 2]) else { return }

        guard let date = dateFormatter.date(from: String(dateSubstring)) else { return }

        // The same activeFormat line repeats several times per track change.
        let key = "\(lastTrackName ?? "?")-\(sampleRate)-\(bitDepth)"
        guard key != lastActiveFormatKey else { return }
        lastActiveFormatKey = key

        let entry = CMEntry(date: date, trackName: lastTrackName, bitDepth: bitDepth, sampleRate: sampleRate)
        entryStream.send(entry)
    }
}
