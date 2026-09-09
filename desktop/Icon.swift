import Cocoa
let size = 1024
let image = NSImage(size: NSSize(width: size, height: size))
image.lockFocus()
let rect = NSRect(x: 45, y: 45, width: 934, height: 934)
let shape = NSBezierPath(roundedRect: rect, xRadius: 218, yRadius: 218)
NSGradient(starting: NSColor(red:0.63,green:0.36,blue:1,alpha:1), ending:NSColor(red:0.36,green:0.12,blue:0.8,alpha:1))!.draw(in:shape,angle:-90)
let inset = NSBezierPath(roundedRect:NSRect(x:170,y:170,width:684,height:684),xRadius:160,yRadius:160)
NSColor.white.withAlphaComponent(0.14).setFill();inset.fill()
let text: NSString = "N"
let attrs: [NSAttributedString.Key:Any] = [.font:NSFont.systemFont(ofSize:520,weight:.bold),.foregroundColor:NSColor.white]
let ts = text.size(withAttributes:attrs)
text.draw(at:NSPoint(x:(1024-ts.width)/2,y:(1024-ts.height)/2+8),withAttributes:attrs)
image.unlockFocus()
let bitmap = NSBitmapImageRep(data:image.tiffRepresentation!)!
try bitmap.representation(using:.png,properties:[:])!.write(to:URL(fileURLWithPath:CommandLine.arguments[1]))
