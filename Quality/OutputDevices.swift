//
//  OutputDevices.swift
//  Quality
//
//  Created by Vincent Neo on 20/4/22.
//

import Combine
import Foundation
import SimplyCoreAudio
import CoreAudio
import CoreAudioTypes
import MediaRemoteAdapter
import OrderedCollections

class OutputDevices: ObservableObject {
    @Published var selectedOutputDevice: AudioDevice? // auto if nil
    @Published var defaultOutputDevice: AudioDevice?
    @Published var outputDevices = [AudioDevice]()
    @Published var currentSampleRate: Float64?
    @Published var currentBitDepth: Int?
    @Published var enableBitDepthDetection = Defaults.shared.userPreferBitDepthDetection
    
    private var enableBitDepthDetectionCancellable: AnyCancellable?
    
    private let coreAudio = SimplyCoreAudio()
    
    private var changesCancellable: AnyCancellable?
    private var defaultChangesCancellable: AnyCancellable?
    private var timerCancellable: AnyCancellable?
    private var outputSelectionCancellable: AnyCancellable?
    private var sampleRateChangeCancellable: AnyCancellable?
    private var unknownSourceTimerCancellable: AnyCancellable?
    // Apps whose rate we handle directly; anything else producing output is "unknown".
    private let handledOutputBundleIDs: Set<String> = ["com.apple.Music", "com.apple.TV", "com.qobuz.desktop"]
    private var unknownSourceStreak = 0
    private var didPinDefaultForUnknown = false
    // Last rate LS set for a handled source (Music/TV/Qobuz). Used to restore the
    // device after an unknown-source pin ends, since an ongoing handled track won't
    // re-log to trigger a fresh switch.
    private var lastHandledRate: Float64?

    private let logReader = LogReader()
    private var entryStreamReceiver: AnyCancellable?
    private var lastTrackChangeTime: Date?
    
    private var collection: OrderedDictionary<String, InfoPair> = [:]
    private var currentTrackPair: DatedPair<MediaTrack>?
    private var updateRequester = PassthroughSubject<Void, Never>()
    private var updateRequesterReceiver: AnyCancellable?
    
    private var pairHandlingQueue = DispatchQueue(label: "phq", qos: .userInteractive)
    
    private var consoleQueue = DispatchQueue(label: "consoleQueue", qos: .userInteractive)
    
    private var processQueue = DispatchQueue(label: "processQueue", qos: .userInitiated)
    
    private var previousSampleRate: Float64?
    private var previousBitDepth: Int?
    var trackAndSample = [MediaTrack : Float64]()
    var trackAndBitDepth = [MediaTrack : Int]()
    var previousTrack: MediaTrack?
    var currentTrack: MediaTrack?
    // Bundle id + play state of the current now-playing app (from MediaRemote).
    // Used to gate direct switching for the TV app, and to tell whether a handled
    // app is actually playing vs. just holding its output stream open while paused
    // (which Electron apps like Qobuz do).
    var currentPlayerBundleID: String?
    var currentPlayerIsPlaying = false

    var timerActive = false
    var timerCalls = 0

    // Single shared instance: the app has both a SwiftUI menu-bar controller and
    // an AppDelegate that need the same OutputDevices. Two instances meant two
    // LogReaders (two `log stream` processes) and the device being switched twice.
    // Lazy access avoids the launch-order trap (nil is impossible — whoever
    // touches it first creates it).
    static let shared = OutputDevices()

    private init() {
        self.outputDevices = self.coreAudio.allOutputDevices
        self.defaultOutputDevice = self.coreAudio.defaultOutputDevice
        self.getDeviceSampleRate()
        
        self.logReader.spawnProcessIfNeeded()
        
        // WIP: This code is dizzying to work with...
        entryStreamReceiver = logReader.entryStream.receive(on: pairHandlingQueue).sink { [weak self] entry in
            //print("ESR", self.lastTrackChangeTime, self.currentTrack, entry.date, entry.trackName, entry.sampleRate)

            // External (non-Music) sources like the TV app have no track-name pairing.
            // Switch the device directly, but only when that app is the active
            // now-playing source (so a background TV decode can't hijack Music).
            if entry.isExternal {
                guard self?.currentPlayerBundleID == "com.apple.TV" else { return }
                let format = AudioFormat(sampleRate: entry.sampleRate, bitDepth: entry.bitDepth)
                self?.switchLatestSampleRate(format: format)
                return
            }

            let key = entry.trackName ?? UUID().uuidString
            
            if entry.trackName == nil, let lastKey = self?.collection.keys.last, let lastDate = self?.collection[lastKey]?.format?.date {
                if abs(lastDate.timeIntervalSince1970 - entry.date.timeIntervalSince1970) < 1 {
                    return
                }
            }
            
            let format = DatedPair(date: entry.date, object: AudioFormat(sampleRate: entry.sampleRate, bitDepth: entry.bitDepth))
            if let pair = self?.collection[key] {
                pair.format = format
            }
            else {
                self?.collection[key] = InfoPair(format: format)
            }
            
            self?.updateRequester.send()
        }
        
        updateRequesterReceiver = updateRequester
            .throttle(for: 0.2, scheduler: DispatchQueue.global(), latest: true)
            .receive(on: pairHandlingQueue)
            .sink { [weak self] in
                guard let collection = self?.collection, let currentTrackPair = self?.currentTrackPair else { return }
                print(collection)
                var limit = 0
                for (_, value) in collection.reversed() {
                    guard limit < 5 else { return }
                    defer {
                        limit += 1
                    }
                    
                    if let track = value.track?.object, let current = self?.currentTrack, track == current {
                        print("URR TRACK YES")
                        if let format = value.format?.object {
                            print("URR FORMAT YES")
                            self?.switchLatestSampleRate(format: format)
                            return
                        }
                    }
                    
//                    let trackValue = value.track
//                    let format = value.format
//                    if let title = currentTrackPair.object.title {
//                        
//                        return
//                    }
                }
        }
        
        changesCancellable =
            NotificationCenter.default.publisher(for: .deviceListChanged).sink(receiveValue: { _ in
                self.outputDevices = self.coreAudio.allOutputDevices
            })
        
        defaultChangesCancellable =
            NotificationCenter.default.publisher(for: .defaultOutputDeviceChanged).sink(receiveValue: { _ in
                self.defaultOutputDevice = self.coreAudio.defaultOutputDevice
                self.getDeviceSampleRate()
            })

        // Keep the menu-bar readout in sync with the device's ACTUAL nominal rate,
        // whoever changes it — LS itself for Apple Music, or another app that
        // drives the device directly (e.g. Qobuz's exclusive mode). SimplyCoreAudio
        // posts this on the kAudioDevicePropertyNominalSampleRate listener.
        sampleRateChangeCancellable =
            NotificationCenter.default.publisher(for: .deviceNominalSampleRateDidChange).sink(receiveValue: { [weak self] notification in
                guard let self else { return }
                // Only reflect the device we're actually outputting to.
                if let changed = notification.object as? AudioDevice,
                   let current = self.selectedOutputDevice ?? self.defaultOutputDevice,
                   changed != current {
                    return
                }
                self.getDeviceSampleRate()
            })

        outputSelectionCancellable = $selectedOutputDevice.sink(receiveValue: { _ in
            self.getDeviceSampleRate()
        })
        
        enableBitDepthDetectionCancellable = Defaults.shared.$userPreferBitDepthDetection.sink(receiveValue: { newValue in
            self.enableBitDepthDetection = newValue
        })

        // Poll the per-process audio API for output from apps we don't handle
        // (browsers, video streaming, games). They don't expose a rate and don't
        // switch the device, so we pin it to 384 kHz for them (ZH3 workaround; see
        // evaluateUnknownSource). Cheap property reads.
        unknownSourceTimerCancellable = Timer.publish(every: 2, on: .main, in: .common)
            .autoconnect()
            .sink { [weak self] _ in
                // Watchdog: respawn the log reader if it died (no-op if alive), so
                // LS recovers from a reaped `log stream` without an app restart.
                self?.logReader.spawnProcessIfNeeded()
                self?.processQueue.async { self?.evaluateUnknownSource() }
            }

    }
    
    deinit {
        changesCancellable?.cancel()
        defaultChangesCancellable?.cancel()
        timerCancellable?.cancel()
        enableBitDepthDetectionCancellable?.cancel()
        sampleRateChangeCancellable?.cancel()
        unknownSourceTimerCancellable?.cancel()
        entryStreamReceiver?.cancel()
        //timer.upstream.connect().cancel()
    }
    
    func renewTimer() {
        if timerCancellable != nil { return }
        timerCancellable = Timer
            .publish(every: 2, on: .main, in: .default)
            .autoconnect()
            .sink { [weak self] _ in
                guard let self = self else { return }
                self.timerCalls += 1
                if self.timerCalls >= 5 {
                    self.timerCalls = 0
                    self.timerCancellable?.cancel()
                    self.timerCancellable = nil
                }
                else {
//                    self.processQueue.async {
//                        self.switchLatestSampleRate()
//                    }
                }
            }
    }
    
    func getDeviceSampleRate() {
        let defaultDevice = self.selectedOutputDevice ?? self.defaultOutputDevice
        guard let sampleRate = defaultDevice?.nominalSampleRate else { return }
        self.updateSampleRate(sampleRate, bitDepth: nil)
    }

    // Bundle ids of every process currently producing audio OUTPUT, via the
    // macOS 14.2+ per-process Core Audio API. Read-only; needs no permission.
    private func outputProducingBundleIDs() -> Set<String> {
        guard #available(macOS 14.2, *) else { return [] }
        let sys = AudioObjectID(kAudioObjectSystemObject)
        var listAddr = AudioObjectPropertyAddress(
            mSelector: kAudioHardwarePropertyProcessObjectList,
            mScope: kAudioObjectPropertyScopeGlobal,
            mElement: kAudioObjectPropertyElementMain)
        var size: UInt32 = 0
        guard AudioObjectGetPropertyDataSize(sys, &listAddr, 0, nil, &size) == noErr else { return [] }
        let count = Int(size) / MemoryLayout<AudioObjectID>.size
        guard count > 0 else { return [] }
        var procs = [AudioObjectID](repeating: 0, count: count)
        guard AudioObjectGetPropertyData(sys, &listAddr, 0, nil, &size, &procs) == noErr else { return [] }

        var producers = Set<String>()
        for proc in procs {
            var outAddr = AudioObjectPropertyAddress(
                mSelector: kAudioProcessPropertyIsRunningOutput,
                mScope: kAudioObjectPropertyScopeGlobal,
                mElement: kAudioObjectPropertyElementMain)
            var isOutput: UInt32 = 0
            var outSize = UInt32(MemoryLayout<UInt32>.size)
            guard AudioObjectGetPropertyData(proc, &outAddr, 0, nil, &outSize, &isOutput) == noErr,
                  isOutput != 0 else { continue }

            var bidAddr = AudioObjectPropertyAddress(
                mSelector: kAudioProcessPropertyBundleID,
                mScope: kAudioObjectPropertyScopeGlobal,
                mElement: kAudioObjectPropertyElementMain)
            var cfBundle: Unmanaged<CFString>? = nil
            var bidSize = UInt32(MemoryLayout<CFString?>.size)
            guard AudioObjectGetPropertyData(proc, &bidAddr, 0, nil, &bidSize, &cfBundle) == noErr,
                  let bundle = cfBundle?.takeRetainedValue() as String?, !bundle.isEmpty else { continue }
            producers.insert(bundle)
        }
        return producers
    }

    // RATE-ARBITRATION POLICY (multiple simultaneous sources): LS keys off the
    // system's single "now-playing" app. Whatever is the active now-playing source
    // wins the rate — Music/TV switch to their native rate only while now-playing;
    // an unknown source gets the pin only when no handled source is now-playing+
    // playing; other background audio just mixes/resamples into whatever the active
    // source set. There is deliberately NO explicit multi-source priority resolver.
    // USER BEWARE: for bit-perfect playback of a lossless track, don't run any other
    // audio source at the same time — a second source (esp. Qobuz, which drives the
    // device itself) can pull the rate off the lossless track's native value.
    //
    // When audio is coming ONLY from an app we don't handle (e.g. a browser),
    // pin the device to 384 kHz — those apps don't report a rate or switch the
    // device themselves. NOTE: normally 48 kHz (the video/web rate) would be the
    // natural choice, but this is pinned high (384 kHz) as a workaround for a Fosi
    // ZH3 firmware bug that drops audio in the silences between speech at normal
    // rates; per Fosi's FAQ, running at 384 kHz avoids it. Revert to 48000 once the
    // ZH3 firmware is updated (the update needs Windows).
    private func evaluateUnknownSource() {
        let producers = outputProducingBundleIDs()
        let unknown = producers
            .subtracting(handledOutputBundleIDs)
            .subtracting(["com.vincent-neo.LosslessSwitcher"])

        // A handled source (Music/TV/Qobuz) is the active now-playing app AND actually
        // playing? Can't use IsRunningOutput — Electron apps (Qobuz) keep their output
        // stream open while paused; MediaRemote's isPlaying is the truth.
        let handledPlaying = (currentPlayerBundleID.map { handledOutputBundleIDs.contains($0) } ?? false)
            && currentPlayerIsPlaying

        // Handled source active → stop pinning. If we HAD pinned for a now-inactive
        // unknown source, restore the handled source's last rate: an ongoing track
        // won't re-log, so nothing else would move the device off the pin.
        if handledPlaying {
            unknownSourceStreak = 0
            if didPinDefaultForUnknown {
                didPinDefaultForUnknown = false
                if let target = lastHandledRate {
                    self.switchLatestSampleRate(format: AudioFormat(sampleRate: Int(target), bitDepth: nil))
                }
            }
            return
        }

        // Nothing unknown producing. Leave the rate as-is, but KEEP the pin flag so a
        // handled source that starts later still restores its rate — don't reset it
        // here, or a briefly-stale now-playing would drop the restore.
        guard !unknown.isEmpty else {
            unknownSourceStreak = 0
            return
        }

        // Unknown source producing, no handled source playing → pin (after a couple
        // of ticks so brief system sounds don't trigger it). isUnknownSourcePin keeps
        // the pin from overwriting the remembered handled rate.
        unknownSourceStreak += 1
        guard unknownSourceStreak >= 2, !didPinDefaultForUnknown else { return }
        didPinDefaultForUnknown = true
        self.switchLatestSampleRate(format: AudioFormat(sampleRate: 384000, bitDepth: 24), isUnknownSourcePin: true)
    }
    
    func getSampleRateFromAppleScript() -> Double? {
        let scriptContents = "tell application \"Music\" to get sample rate of current track"
        var error: NSDictionary?
        
        if let script = NSAppleScript(source: scriptContents) {
            let output = script.executeAndReturnError(&error).stringValue
            
            if let error = error {
                print("[APPLESCRIPT] - \(error)")
            }
            guard let output = output else { return nil }

            if output == "missing value" {
                return nil
            }
            else {
                return Double(output)
            }
        }
        
        return nil
    }
    
    func getAllStats() -> [CMPlayerStats] {
        var allStats = [CMPlayerStats]()
        
        do {
//            let musicLogs = try Console.getRecentEntries(type: .music)
            let coreAudioLogs = try Console.getRecentEntries(type: .coreAudio)
//            let coreMediaLogs = try Console.getRecentEntries(type: .coreMedia)
            
//            allStats.append(contentsOf: CMPlayerParser.parseMusicConsoleLogs(musicLogs))
//            if enableBitDepthDetection {
                allStats.append(contentsOf: CMPlayerParser.parseCoreAudioConsoleLogs(coreAudioLogs))
//            }
//            else {
//                allStats.append(contentsOf: CMPlayerParser.parseCoreMediaConsoleLogs(coreMediaLogs))
//            }

//            allStats.sort(by: {$0.priority > $1.priority})
            print("[getAllStats] \(allStats)")
        }
        catch {
            print("[getAllStats, error] \(error)")
        }
        
        return allStats
    }
    
    func switchLatestSampleRate(format: AudioFormat, isUnknownSourcePin: Bool = false) {
        let defaultDevice = self.selectedOutputDevice ?? self.defaultOutputDevice
        
        if let supported = defaultDevice?.nominalSampleRates {
            let sampleRate = Float64(format.sampleRate)
            let bitDepth = Int32(format.bitDepth ?? 32)
            
//            if self.currentTrack == self.previousTrack, let prevSampleRate = currentSampleRate, prevSampleRate > sampleRate {
//                print("same track, prev sample rate is higher")
//                return
//            }
//            
//            if sampleRate == 48000 && !recursion {
//                processQueue.asyncAfter(deadline: .now() + 1) {
//                    self.switchLatestSampleRate(recursion: true)
//                }
//            }
            
            let formats = self.getFormats(device: defaultDevice!)!
            
            // https://stackoverflow.com/a/65060134
            var nearest = supported.min(by: {
                abs($0 - sampleRate) < abs($1 - sampleRate)
            })
            
            let nearestBitDepth = formats.min(by: {
                abs(Int32($0.mBitsPerChannel) - bitDepth) < abs(Int32($1.mBitsPerChannel) - bitDepth)
            })
            
            if Defaults.shared.userPreferSampleRateMultiples,
               let nearestSampleRate = nearest,
               nearestSampleRate != sampleRate, supported.contains(sampleRate / 2) {
                nearest = sampleRate / 2
            }
            
            let nearestFormat = formats.filter({
                $0.mSampleRate == nearest && $0.mBitsPerChannel == nearestBitDepth?.mBitsPerChannel
            })
            
            print("NEAREST FORMAT \(nearestFormat)")
            
            if let suitableFormat = nearestFormat.first {
                if enableBitDepthDetection {
                    self.setFormats(device: defaultDevice, format: suitableFormat)
                }
                else if suitableFormat.mSampleRate != previousSampleRate { // bit depth disabled
                    defaultDevice?.setNominalSampleRate(suitableFormat.mSampleRate)
                }
                self.updateSampleRate(suitableFormat.mSampleRate, bitDepth: Int(suitableFormat.mBitsPerChannel))
                // Remember a real handled-source rate so we can restore it after an
                // unknown-source pin (the pin itself must not overwrite it).
                if !isUnknownSourcePin {
                    self.lastHandledRate = suitableFormat.mSampleRate
                }
                if let currentTrack = currentTrack {
                    self.trackAndSample[currentTrack] = suitableFormat.mSampleRate
                    self.trackAndBitDepth[currentTrack] = Int(suitableFormat.mBitsPerChannel)
                }
            }

//            if let nearest = nearest {
//                let nearestSampleRate = nearest.element
//                if nearestSampleRate != previousSampleRate {
//                    defaultDevice?.setNominalSampleRate(nearestSampleRate)
//                    self.updateSampleRate(nearestSampleRate)
//                    if let currentTrack = currentTrack {
//                        self.trackAndSample[currentTrack] = nearestSampleRate
//                    }
//                }
//            }
        }
//        else if !recursion {
//            processQueue.asyncAfter(deadline: .now() + 1) {
//                self.switchLatestSampleRate(recursion: true)
//            }
//        }
        else {
//                print("cache \(self.trackAndSample)")
            if self.currentTrack == self.previousTrack {
                print("same track, ignore cache")
                return
            }
//            if let currentTrack = currentTrack, let cachedSampleRate = trackAndSample[currentTrack] {
//                print("using cached data")
//                if cachedSampleRate != previousSampleRate {
//                    defaultDevice?.setNominalSampleRate(cachedSampleRate)
//                    self.updateSampleRate(cachedSampleRate)
//                }
//            }
        }

    }
    
    func getFormats(device: AudioDevice) -> [AudioStreamBasicDescription]? {
        // new sample rate + bit depth detection route
        let streams = device.streams(scope: .output)
        let availableFormats = streams?.first?.availablePhysicalFormats?.compactMap({$0.mFormat})
        return availableFormats
    }
    
    func setFormats(device: AudioDevice?, format: AudioStreamBasicDescription?) {
        guard let device, let format else { return }
        let streams = device.streams(scope: .output)
        if streams?.first?.physicalFormat != format {
            streams?.first?.physicalFormat = format
        }
    }
    
    func updateSampleRate(_ sampleRate: Float64, bitDepth: Int?) {
        self.previousSampleRate = sampleRate
        self.previousBitDepth = bitDepth
        DispatchQueue.main.async { [self] in
            let readableSampleRate = sampleRate / 1000
            self.currentSampleRate = readableSampleRate
            self.currentBitDepth = bitDepth
            
            let delegate = AppDelegate.instance
            
            if enableBitDepthDetection {
                if let bitDepth = bitDepth {
                    delegate?.statusItemTitle = String(format: "%.1f kHz / %d bit", readableSampleRate, bitDepth)
                } else {
                    delegate?.statusItemTitle = String(format: "%.1f kHz / ? bit", readableSampleRate)
                }
            } else {
                delegate?.statusItemTitle = String(format: "%.1f kHz", readableSampleRate)
            }
        }
        self.runUserScript(sampleRate, bitDepth: bitDepth)
    }
    
    func runUserScript(_ sampleRate: Float64, bitDepth: Int?) {
        guard let scriptPath = Defaults.shared.shellScriptPath else { return }
        let argumentSampleRate = String(Int(sampleRate))
        var arguments = [argumentSampleRate]
        
        // Add bit depth as second argument if available
        if let bitDepth = bitDepth {
            arguments.append(String(bitDepth))
        }
        
        Task.detached {
            let scriptURL = URL(fileURLWithPath: scriptPath)
            do {
                let task = try NSUserUnixTask(url: scriptURL)
                try await task.execute(withArguments: arguments)
            }
            catch {
                print("TASK ERR \(error)")
            }
        }
    }
    
    func trackDidChange(_ newTrack: TrackInfo) {
        let mt = MediaTrack(trackInfo: newTrack)
        // Track which app is now-playing and whether it's actually playing (set
        // before the same-track guard so play/pause of the same item updates it).
        self.currentPlayerBundleID = newTrack.payload.bundleIdentifier
        self.currentPlayerIsPlaying = newTrack.payload.isPlaying ?? false

        guard previousTrack != mt else { return }
        self.previousTrack = self.currentTrack
        self.currentTrack = mt

        pairHandlingQueue.async { [weak self] in
            let now = Date.now
            let pair = DatedPair(date: now, object: mt)
            self?.currentTrackPair = pair
            
            let key = mt.title ?? UUID().uuidString
            if let collectionPair = self?.collection[key] {
                collectionPair.track = pair
            }
            else if let lastKey = self?.collection.keys.last, self?.collection[lastKey]?.track == nil, UUID(uuidString: lastKey) != nil {
                self?.collection[lastKey]?.track = pair
            }
            else {
                self?.collection[key] = .init(track: pair)
            }
            self?.updateRequester.send()
        }
        
        
//        if self.previousTrack != self.currentTrack {
//            self.renewTimer()
//        }
//        processQueue.async { [unowned self] in
//            self.switchLatestSampleRate()
//        }
        lastTrackChangeTime = Date()
    }
}
