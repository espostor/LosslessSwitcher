//
//  CMEntry.swift
//  LosslessSwitcher
//
//  Created by Vincent Neo on 1/3/26.
//

import Foundation

struct CMEntry {
    let date: Date
    let trackName: String?
    let bitDepth: Int?
    let sampleRate: Int
    // true when the rate comes from a non-Music Apple app (e.g. the TV app),
    // which has no track-name pairing — switch the device directly instead.
    var isExternal: Bool = false
}
